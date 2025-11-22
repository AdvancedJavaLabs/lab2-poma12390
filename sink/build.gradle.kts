plugins {
    application
}

dependencies {
    implementation(project(":common"))

    // RabbitMQ client
    implementation("com.rabbitmq:amqp-client:5.22.0")

    // Jackson для JSON
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.0")
}

application {
    mainClass.set("itmo.maga.javaparallel.lab2.sink.ResultSinkApp")
}
