package ru.quipy.payments.logic

import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.slf4j.LoggerFactory

import java.util.*
import java.util.concurrent.*
import java.net.SocketTimeoutException
import java.time.Duration

import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.common.utils.definitions.HttpStatusRetryable

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val HTTP_CLIENT_TIMEOUT = 1200L
        val PAYMENT_PROVIDER_TIMEOUT = Duration.ofSeconds(1)

        val MAX_RETRIES = 3
        val RETRY_DELAY = 1000L

        val CORE_POOL_SIZE = 4
        val MAX_CORE_POOL_SIZE = 6
        val POOL_QUEUE_SIZE = 50
        val KEEP_ALIVE_TIME = 5L
        val KEEP_ALIVE_TIME_UNIT = TimeUnit.MINUTES

        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder()
        .callTimeout(2000, TimeUnit.MILLISECONDS)
        .build()

    private val rateLimiter = TokenBucketRateLimiter(
        rate = rateLimitPerSec,
        bucketMaxCapacity = rateLimitPerSec,
        window = 1,
        timeUnit = TimeUnit.SECONDS
    )

    private val ongoingWindow = NonBlockingOngoingWindow(parallelRequests)

    private val pool = ThreadPoolExecutor(
        CORE_POOL_SIZE,
        MAX_CORE_POOL_SIZE,
        KEEP_ALIVE_TIME,
        KEEP_ALIVE_TIME_UNIT,
        LinkedBlockingQueue(POOL_QUEUE_SIZE),
        Executors.defaultThreadFactory(),
        ThreadPoolExecutor.AbortPolicy()
    )

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        pool.submit {
            logger.warn("[$accountName] Submitting payment request for payment $paymentId")

            val transactionId = UUID.randomUUID()
            logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            while (ongoingWindow.putIntoWindow() is NonBlockingOngoingWindow.WindowResponse.Fail || !rateLimiter.tick()) {
                if (checkDeadline(deadline, paymentId, transactionId, requestAverageProcessingTime.toMillis())) {
                    return@submit
                }
            }

            var attempt = 0
            val request = Request.Builder().run {
                url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount&timeout=$PAYMENT_PROVIDER_TIMEOUT")
                post(emptyBody)
            }.build()

            while (attempt < MAX_RETRIES) {
                val call = client.newCall(request)
                try {
                    if (checkDeadline(deadline, paymentId, transactionId)) {
                        return@submit
                    }

                    var isRetry = false
                    call.execute().use { response ->
                        if (HttpStatusRetryable.fromCode(response.code) != null) {
                            logger.warn("[$accountName] Retrying payment for txId: $transactionId, payment: $paymentId, response code: ${response.code}")
                            attempt++
                            Thread.sleep(RETRY_DELAY)

                            if (checkDeadline(deadline, paymentId, transactionId)) {
                                return@submit
                            }

                            isRetry = MAX_RETRIES - attempt > 1
                        }

                        if (checkDeadline(deadline, paymentId, transactionId)) {
                            return@submit
                        }

                        if (!isRetry) {
                            val body = try {
                                mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                            } catch (e: Exception) {
                                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                            }

                            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                            paymentESService.update(paymentId) {
                                it.logProcessing(body.result, now(), transactionId, reason = body.message)
                            }

                            return@submit
                        }
                    }
                } catch (e: Exception) {
                    when (e) {
                        is SocketTimeoutException -> {
                            logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                            }
                        }
                        else -> {
                            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                        }
                    }
                    return@submit
                } finally {
                    ongoingWindow.releaseWindow()
                    if (System.currentTimeMillis() >= deadline) {
                        call.cancel()
                    }
                }
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private fun checkDeadline(deadline: Long, paymentId: UUID, transactionId: UUID, timeMultiplier: Long = 0): Boolean {
        if (System.currentTimeMillis() + timeMultiplier >= deadline) {
            logger.warn("[$accountName] Rate or parallel requests limit timeout for payment $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Rate limit exceeded")
            }

            return true
        }

        return false
    }
}

public fun now() = System.currentTimeMillis()