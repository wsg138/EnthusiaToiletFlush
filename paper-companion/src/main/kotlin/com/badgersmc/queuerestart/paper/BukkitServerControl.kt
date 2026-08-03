package com.badgersmc.queuerestart.paper

import org.bukkit.Bukkit

/** Paper-bound lifecycle control. Only a clean Bukkit shutdown is exposed. */
class BukkitServerControl : ServerControl {
    override fun shutdown() {
        Bukkit.shutdown()
    }
}
