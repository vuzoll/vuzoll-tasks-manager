package com.github.vuzoll.tasks

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.AutoConfigureAfter
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration
import org.springframework.boot.autoconfigure.data.mongo.MongoRepositoriesAutoConfiguration
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration
import org.springframework.core.task.SimpleAsyncTaskExecutor
import org.springframework.core.task.TaskExecutor

@Configuration
@AutoConfigureAfter([ MongoAutoConfiguration, MongoDataAutoConfiguration, MongoRepositoriesAutoConfiguration ])
@EnableConfigurationProperties(TasksManagerProperties)
class TasksManagerAutoConfiguration {

    @Autowired
    TasksManagerProperties tasksManagerProperties

    @ConditionalOnMissingBean(annotation = TaskExecutorBean)
    @TaskExecutorBean
    TaskExecutor taskExecutor() {
        new SimpleAsyncTaskExecutor()
    }
}
