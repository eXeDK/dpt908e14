import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import scala.util.Random

//WorkerType
//////////
object WorkerType extends Enumeration {
  type WorkerType = Value
  val Elf, Reindeer = Value
}

//Worker
////////
class Worker(
  workerType: WorkerType.WorkerType,
  id: Int,
  maxSleepTime: Int,
  santasHouse: ActorRef) extends Actor {
  //Forces the first work iteration and going to Santa
  self ! Unit

  def receive = {
    //Receive is used for blocking the worker while it meets with Santa and he
    //sends one type of message, but cases are need to satisfy the type system
    case Unit =>
      Main.sleepRandomInterval(maxSleepTime)
      santasHouse ! (workerType, id)
  }
}

//Reindeer and Elf
class Reindeer(id: Int, santasHouse: ActorRef)
  extends Worker(WorkerType.Reindeer, id, 30, santasHouse)

class Elf(id: Int, santasHouse: ActorRef)
  extends Worker(WorkerType.Elf, id, 15, santasHouse)

//Santa
///////
class SantasHouse(santa: ActorRef) extends Actor {
  var elfs = List[(Int, ActorRef)]()
  var reindeers = List[(Int, ActorRef)]()

  def enterQueue(
    workerType: WorkerType.WorkerType,
    id: Int,
    actorRef: ActorRef,
    queue: List[(Int, ActorRef)],
    maxQueue: Int): List[(Int, ActorRef)] = {

    val updatedQueue = (id, actorRef) +: queue
    if (updatedQueue.length == maxQueue) {
      val (ids, actorRefs) = updatedQueue.unzip
      santa ! (workerType, ids, actorRefs)
      return List[(Int, ActorRef)]()
    }
    updatedQueue
  }

  def receive = {
    case (WorkerType.Reindeer, id: Int) =>
      reindeers = enterQueue(WorkerType.Reindeer, id, sender, reindeers, 9)
    case (WorkerType.Elf, id: Int) =>
      elfs = enterQueue(WorkerType.Elf, id, sender, elfs, 3)
  }
}


//Santa
///////
class Santa extends Actor {
  def receive = {
    case (WorkerType.Reindeer, ids: List[Int], actorRefs: List[ActorRef]) =>
      println(s"Santa: ho ho delivering presents with reindeers: $ids")
      Main.sleepRandomInterval(5)
      actorRefs.foreach(ref => ref ! Unit)
    case (WorkerType.Elf, ids: List[Int], actorRefs: List[ActorRef]) =>
      println(s"Santa: ho ho helping elfs: $ids")
      Main.sleepRandomInterval(5)
      actorRefs.foreach(ref => ref ! Unit)
  }
}

object Main {
  def sleepRandomInterval(maxTime: Int): Unit = {
    val sleepTime = Random.nextInt(maxTime)
    Thread.sleep(sleepTime * 1000)
  }

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("greenland")
    val santa = system.actorOf(Props(new Santa))
    val santasHouse = system.actorOf(Props(new SantasHouse(santa)))
    (1 to 9) foreach(id =>
        system.actorOf(Props(new Reindeer(id, santasHouse))))
    (1 to 30) foreach(id =>
        system.actorOf(Props(new Elf(id, santasHouse))))
  }
}
