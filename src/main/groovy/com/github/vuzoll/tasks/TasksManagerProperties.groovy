package com.github.vuzoll.tasks

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties('vuzoll.tasks')
class TasksManagerProperties {

    String executorQualifier = 'default'
}
