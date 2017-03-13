package com.github.vuzoll.tasks.service

abstract class DurableJob {

    final String name

    boolean finished

    DurableJob(String name) {
        this.name = name
        this.finished = false
    }

    void initSelf(Closure statusUpdater) {
        // do nothing by default
    }

    abstract void doSomething(Closure statusUpdater)

    void markFinished() {
        this.finished = true
    }
}
