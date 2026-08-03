package com.badgersmc.queuerestart.velocity.domain.id

import java.util.UUID

/** A backend server name as registered with the Velocity proxy. */
@JvmInline
value class ServerId(val value: String) {
    init {
        require(value.matches(Regex("[A-Za-z0-9_.-]{1,64}"))) {
            "ServerId must match [A-Za-z0-9_.-]{1,64}"
        }
    }
}

/** A player's UUID — opaque to domain logic. */
@JvmInline
value class PlayerId(val uuid: UUID)
