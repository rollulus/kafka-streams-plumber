import com.eneco.energy.kafka.streams.plumber.Logging
import org.luaj.vm2.LuaValue
import org.luaj.vm2.lib._

class log() extends TwoArgFunction with Logging {
  override def call(modname: LuaValue, env:LuaValue ):LuaValue = {
    val library = LuaValue.tableOf()
    library.set( "debug", new ginfo(log.debug) )
    library.set( "trace", new ginfo(log.trace) )
    library.set( "info", new ginfo(log.info) )
    library.set( "warn", new ginfo(log.warn) )
    library.set( "error", new ginfo(log.error) )
    return library
  }
}

class ginfo(f:String => Unit) extends OneArgFunction with Logging {
  override def call(v: LuaValue):LuaValue = {
    f(v.tojstring)
    this
  }
}
