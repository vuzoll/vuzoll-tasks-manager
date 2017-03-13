package com.github.vuzoll.tasks.service

abstract class DurableJob {

    final String name

    boolean finished

    DurableJob(String name) {
        this.name = name
        this.finished = false
    }

    abstract void initSelf(Closure statusUpdater)

    abstract void doSomething(Closure statusUpdater)

    void markFinished() {
        this.finished = true
    }
}
