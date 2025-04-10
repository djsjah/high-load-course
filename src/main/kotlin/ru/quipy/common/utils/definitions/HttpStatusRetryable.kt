package ru.quipy.common.utils.definitions

enum class HttpStatusRetryable(val code: Int) {
    TOO_MANY_REQUESTS(429),
    INTERNAL_SERVER_ERROR(500),
    BAD_GATEWAY(502),
    SERVICE_UNAVAILABLE(503),
    GATEWAY_TIMEOUT(504);

    companion object {
        fun fromCode(code: Int): HttpStatusRetryable? {
            return values().find { it.code == code }
        }
    }
}