package ru.quipy.payments.logic

import okhttp3.*
import okhttp3.Protocol
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.slf4j.LoggerFactory

import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit

import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val retryCount = 1

    private val rateLimiter: RateLimiter =
        SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow: OngoingWindow = OngoingWindow(parallelRequests)

    private val virtualThreadPool: ExecutorService =
        Executors.newVirtualThreadPerTaskExecutor()

    private val client = OkHttpClient.Builder()
        .protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
        .dispatcher(
            Dispatcher().apply {
                maxRequests = parallelRequests
                maxRequestsPerHost = parallelRequests
            }
        )
        .connectionPool(
            ConnectionPool(
                maxIdleConnections = parallelRequests + 1000,
                keepAliveDuration = 10,
                timeUnit = TimeUnit.MINUTES
            )
        )
        .callTimeout(Duration.ofSeconds(requestAverageProcessingTime.seconds * 3))
        .build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        virtualThreadPool.submit {
            doPerformPayment(paymentId, amount, paymentStartedAt, deadline)
        }
    }

    private fun doPerformPayment(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Skipping payment $paymentId as the account is disabled.")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=$serviceName&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        var currentTry = 1

        try {
            ongoingWindow.acquire()

            fun sendRequest() {
                if (currentTry > retryCount) {
                    ongoingWindow.release()
                    return
                }

                if (!rateLimiter.tick()) {
                    Thread.sleep(1)
                    return sendRequest()
                }

                val expectedFinishTime = now() + requestAverageProcessingTime.toMillis()
                if (expectedFinishTime > deadline) {
                    logger.warn("[$accountName] Skip txId=$transactionId due to deadline timeout before sending request")
                    ongoingWindow.release()
                    return
                }

                client.newCall(request).enqueue(object : Callback {
                    override fun onFailure(call: Call, e: IOException) {
                        logger.error("[$accountName] Request failed: txId=$transactionId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                        currentTry++
                        sendRequest()
                    }

                    override fun onResponse(call: Call, response: Response) {
                        response.use {
                            val responseBody = it.body?.string()
                            val body = try {
                                mapper.readValue(responseBody, ExternalSysResponse::class.java)
                            } catch (e: Exception) {
                                logger.error("[$accountName] JSON parse error for txId=$transactionId", e)
                                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "Invalid JSON: ${e.message}")
                            }

                            logger.warn("[$accountName] Payment processed for txId=$transactionId: ${body.result}, msg: ${body.message}")

                            paymentESService.update(paymentId) {
                                it.logProcessing(body.result, now(), transactionId, reason = body.message)
                            }

                            if (!body.result) {
                                currentTry++
                                sendRequest()
                            } else {
                                ongoingWindow.release()
                            }
                        }
                    }
                })
            }

            sendRequest()

        } catch (e: Exception) {
            logger.error("[$accountName] Critical error during payment $paymentId", e)
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Unexpected error: ${e.message}")
            }
            ongoingWindow.release()
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()