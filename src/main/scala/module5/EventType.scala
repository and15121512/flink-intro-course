package module5

case class EventType(name: String) {
  def getValue(): String = name
}

object EventType {
  val Install = EventType("install")
  val Uninstall = EventType("uninstall")
  val PageView = EventType("page_view")
  val Error = EventType("error")

  def getByName(name: String): Option[EventType] = {
    name match {
      case "install" => Some(Install)
      case "uninstall" => Some(Uninstall)
      case "page_view" => Some(PageView)
      case "Error" => Some(Error)
      case _ => None
    }
  }
}
