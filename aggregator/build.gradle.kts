plugins {
    application
}

dependencies {
    implementation(project(":common"))
    implementation("com.rabbitmq:amqp-client:5.22.0")
}

application {
    mainClass.set("itmo.maga.javaparallel.lab2.aggregator.AggregatorApp")
}
