import java.sql.Timestamp

import akka.actor.{ActorSystem, Props}
import akka.persistence.fsm.PersistentFSM
import akka.persistence.fsm.PersistentFSM.FSMState
import akka.util.Timeout
import akka.pattern.ask

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.{ClassTag, classTag}
import scala.util.Random

/**
  * Created by Tracy on 10/01/2017.
  */
///////////////////////////////////////////////////////////
// FSM Command Definition Section
///////////////////////////////////////////////////////////
case object Start

case class ProgressUpdate(jobUpdate: JobUpdate)

case object Expire

case object DemoReset

///////////////////////////////////////////////////////////
// FSM State Definition Section
///////////////////////////////////////////////////////////
sealed trait AccountBalanceRechargeJobState extends FSMState

//the initial state
case object New extends AccountBalanceRechargeJobState {
  override def identifier: String = "New"
}

//children created
case object Initialized extends AccountBalanceRechargeJobState {
  override def identifier: String = "Initialized"
}

//process kicked off, while receiving updates
case object Processing extends AccountBalanceRechargeJobState {
  override def identifier: String = "Processing"
}

//There might be more 'OutOfDate' State
case object Expired extends AccountBalanceRechargeJobState {
  override def identifier: String = "Expired"
}

//
case object Completed extends AccountBalanceRechargeJobState {
  override def identifier: String = "Completed"
}

///////////////////////////////////////////////////////////
// Date Definition Section
///////////////////////////////////////////////////////////
sealed trait AccountBalanceRechargeJobData {
  def willBeDoneWith(jobUpdate: JobUpdate): Boolean

  def withJobUpdate(update: JobUpdate): AccountBalanceRechargeJobData
}

case class JobOutline(jobNumber: String,
                      rechargeClosedTime: Timestamp,
                      fundPurchaseClosedTime: Timestamp,
                      salaryPlanSelected: Int,
                      hfBankAccountSelected: Int, otherBanksAccountSelected: Int,
                      hfBankAccountSelectedAmount: BigDecimal, otherBanksAccountSelectedAmount: BigDecimal)
  extends AccountBalanceRechargeJobData {
  override def withJobUpdate(update: JobUpdate): AccountBalanceRechargeJobData = JobProcessingStatus(this, update)

  override def willBeDoneWith(jobUpdate: JobUpdate): Boolean = {
    //this is not accurate, need to be fixed
    salaryPlanSelected <= (jobUpdate.hfBankAccountFundPurchased + jobUpdate.otherBanksAccountFundPurchased)
  }
}

case class JobProcessingStatus(outline: JobOutline, updateAcc: JobUpdate)
  extends AccountBalanceRechargeJobData {
  override def withJobUpdate(another: JobUpdate): AccountBalanceRechargeJobData = copy(updateAcc = updateAcc.add(another))

  override def willBeDoneWith(jobUpdate: JobUpdate): Boolean = {
    //this is not accurate, need to be fixed
    outline.salaryPlanSelected == (
      jobUpdate.hfBankAccountFundPurchased + jobUpdate.otherBanksAccountFundPurchased +
        updateAcc.hfBankAccountFundPurchased + updateAcc.otherBanksAccountFundPurchased
      )
  }
}


//value object reused by Events
case class JobUpdate(batchCreated: Int = 0, //there should be more numbers for more indicators
                     hfBankAccountRecharged: Int = 0, otherBanksAccountRecharged: Int = 0,
                     hfBankAccountUnknownRecharged: Int = 0, otherBanksAccountUnknownRecharged: Int = 0,
                     hfBankAccountFundPurchased: Int = 0, otherBanksAccountFundPurchased: Int = 0) {
  def add(another: JobUpdate) = copy(
    batchCreated = this.batchCreated + another.batchCreated,
    hfBankAccountRecharged = this.hfBankAccountRecharged + another.hfBankAccountRecharged,
    otherBanksAccountRecharged = this.otherBanksAccountRecharged + another.otherBanksAccountRecharged,
    //...
    hfBankAccountFundPurchased = this.hfBankAccountFundPurchased + another.hfBankAccountFundPurchased,
    otherBanksAccountFundPurchased = this.otherBanksAccountFundPurchased + another.otherBanksAccountFundPurchased
  )
}

///////////////////////////////////////////////////////////
// Domain Event Definition Section
///////////////////////////////////////////////////////////

sealed trait AccountBalanceRechargeJobDomainEvent

case class InitEvent(jobOutline: JobOutline) extends AccountBalanceRechargeJobDomainEvent

case class UpdateEvent(incrementalProgress: JobUpdate) extends AccountBalanceRechargeJobDomainEvent

case object DemoResetEvent extends AccountBalanceRechargeJobDomainEvent

//there might be a timer to check expiration
case object ExpireEvent extends AccountBalanceRechargeJobDomainEvent

class AccountBalanceRechargeJob extends PersistentFSM[AccountBalanceRechargeJobState, AccountBalanceRechargeJobData, AccountBalanceRechargeJobDomainEvent] {

  override def applyEvent(domainEvent: AccountBalanceRechargeJobDomainEvent, currentData: AccountBalanceRechargeJobData): AccountBalanceRechargeJobData = {
    domainEvent match {
      case InitEvent(jobOutline) =>
        val data = currentData
        println(data)
        data
      case UpdateEvent(incrementalProgress: JobUpdate) =>
        println(s"Progress update: ${incrementalProgress}")
        currentData.withJobUpdate(incrementalProgress)
      case DemoResetEvent =>
        deleteMessages(1000)
        println("reset")
        currentData
    }
  }

  override def persistenceId: String = "AccountBalanceRechargeJob-theOnlyDemoJobID"

  override def domainEventClassTag: ClassTag[AccountBalanceRechargeJobDomainEvent] = classTag[AccountBalanceRechargeJobDomainEvent]

  val interval = 12 * 3600 * 1000 //milli seconds for half day

  def createJobOutline() = JobOutline(
    jobNumber = "theOnlyDemoJobID",
    rechargeClosedTime = new Timestamp(System.currentTimeMillis + interval),
    fundPurchaseClosedTime = new Timestamp(System.currentTimeMillis + 2 * interval),
    salaryPlanSelected = 5,
    hfBankAccountSelected = 2,
    otherBanksAccountSelected = 3,
    hfBankAccountSelectedAmount = 200000,
    otherBanksAccountSelectedAmount = 300000
  )

  val jobOutline = createJobOutline()

  startWith(New, jobOutline)

  when(New) {
    case Event(Start, jobOutline: JobOutline) =>
      println("STARTING IDLE")
      goto(Initialized) applying InitEvent(jobOutline)
  }

  when(Initialized) {
    case Event(ProgressUpdate(jobUpdate), jobOutline: JobOutline) => goto(Processing) applying (UpdateEvent(jobUpdate))
  }

  private def everythingIsDone(currentData: AccountBalanceRechargeJobData, jobUpdate: JobUpdate): Boolean = currentData.willBeDoneWith(jobUpdate)

  when(Processing) {
    case Event(ProgressUpdate(jobUpdate), currentProgress: JobProcessingStatus) if everythingIsDone(currentProgress, jobUpdate) => goto(Completed) applying (UpdateEvent(jobUpdate)) andThen {
      case _ =>
      // update database, this is side effects
    }
    case Event(ProgressUpdate(jobUpdate), currentProgress: JobProcessingStatus) if !everythingIsDone(currentProgress, jobUpdate) => stay applying (UpdateEvent(jobUpdate)) andThen {
      case _ =>
      // update database, this is side effects
    }
    case Event(Expire, _) => goto(Expired) applying (ExpireEvent) andThen {
      case _ =>
      // update database, this is side effects
    }
  }

  when(Expired) {
    case Event(ProgressUpdate(jobUpdate), currentProgress: JobProcessingStatus) if everythingIsDone(currentProgress, jobUpdate) => goto(Completed) applying (UpdateEvent(jobUpdate)) andThen {
      case _ =>
      // update database, this is side effects
    }
    case Event(ProgressUpdate(jobUpdate), currentProgress: JobProcessingStatus) if !everythingIsDone(currentProgress, jobUpdate) => stay applying (UpdateEvent(jobUpdate)) andThen {
      case _ =>
      // update database, this is side effects
    }
  }

  when(Completed) {
    case Event(DemoReset, _) => stay applying DemoResetEvent
    case Event(e, _) => stay andThen { case _ => println(s"receiving event: ${e} already completed") }
  }

  override def onRecoveryCompleted() = {
    super.onRecoveryCompleted()
    println("Recovery Completed." + stateName + " data: " + stateData)
  }

  initialize()

}


object AccountBalanceRechargeJob extends App {

  val system = ActorSystem()

  val actor = system.actorOf(Props[AccountBalanceRechargeJob])

  implicit val timeout = Timeout(5 seconds)

  val reset: Future[_] = if (args.length > 0 && args(0) == "reset") actor ? DemoReset else Future("CONTINUE")

  reset.onComplete { response =>
    println(response)
    actor ! Start
    actor ! ProgressUpdate(JobUpdate(hfBankAccountRecharged = 1))
    actor ! ProgressUpdate(JobUpdate(otherBanksAccountRecharged = 1))
    actor ! ProgressUpdate(JobUpdate(hfBankAccountFundPurchased = 1))
    actor ! ProgressUpdate(JobUpdate(otherBanksAccountFundPurchased = 1))

  }

  Thread.sleep(3000)
  system.terminate()

}