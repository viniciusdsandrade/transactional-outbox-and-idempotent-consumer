rootProject.name = "transactional-outbox-idempotency-lab"

include(
    "services:orders-service",
    "services:payments-service",
    "services:billing-service",
)
