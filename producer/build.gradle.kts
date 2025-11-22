plugins {
    application
}

dependencies {
    implementation(project(":common"))

    // RabbitMQ client
    implementation("com.rabbitmq:amqp-client:5.22.0")
}

application {
    // Главный класс продюсера
    mainClass.set("itmo.maga.javaparallel.lab2.producer.ProducerApp")
}
