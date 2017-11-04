package rcms.fm.app.gemfm;

import java.net.URI;
import java.net.URISyntaxException;

import rcms.fm.fw.parameter.FunctionManagerParameter;
import rcms.fm.fw.parameter.type.StringT;

import rcms.fm.fw.user.UserActionException;
import rcms.fm.fw.user.UserEventHandler;

import rcms.fm.resource.QualifiedResource;

import rcms.stateFormat.StateNotification;
import rcms.statemachine.definition.State;

import rcms.util.logger.RCMSLogger;

import rcms.utilities.fm.task.Task;
import rcms.utilities.fm.task.TaskSequence;

/**
 * StateNotificationHandler for GEM
 * Copied from HCAL
 * @author Seth I. Cooper
 * @maintainer Jared Sturdy
 */
public class GEMStateNotificationHandler extends UserEventHandler  {

    static RCMSLogger logger = new RCMSLogger(GEMStateNotificationHandler.class);

    private GEMFunctionManager m_gemFM = null;

    private TaskSequence m_taskSequence = null;

    private Boolean m_isTimeoutActive = false;

    private Thread m_timeoutThread = null;

    private Task m_activeTask = null;

    public GEMStateNotificationHandler()
        throws rcms.fm.fw.EventHandlerException
    {
        String msgPrefix = "[GEM FM] GEMStateNotificationHandler::GEMStateNotificationHandler(): ";
        subscribeForEvents(StateNotification.class);
        addAnyStateAction("processNotice");
    }


    public void init()
        throws rcms.fm.fw.EventHandlerException
    {
        m_gemFM = (GEMFunctionManager) getUserFunctionManager();
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMStateNotificationHandler::GEMStateNotificationHandler(): ";
    }

    // State notification callback
    public void processNotice(Object notice)
        throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMStateNotificationHandler::processNotice(): ";

        StateNotification notification = (StateNotification)notice;
        String            actualState  = m_gemFM.getState().getStateString();

        logger.info(msgPrefix + "current state is: " + actualState
                    + ", processing state notification: " + notification
                    + ", taskSequence: " + m_taskSequence
                    + ", activeTask: " + m_activeTask);
                    // + ", isCompleted: " + m_activeTask.isCompleted());

        if (m_gemFM.getState().equals(GEMStates.ERROR)) {
            String msg = "is in error state: " + m_gemFM.getState();
            logger.warn(msgPrefix + msg);
            return;
        }

        try {
            if (notice instanceof TaskSequence) {
                if (m_activeTask != null) {
                    String msg = "activeList not null in first invocatiion";
                    logger.error(msgPrefix + msg);
                    throw new UserActionException(msg);
                }
                if (m_taskSequence != null) {
                    String msg = "taskSequence not null in first invocation";
                    logger.error(msgPrefix + msg);
                    throw new UserActionException(msg);
                }
                m_taskSequence = (TaskSequence)notice;

                if (m_taskSequence.isEmpty()) {
                    String msg = "New task notification received but it is empty";
                    logger.error(msgPrefix + msg);
                    throw new UserActionException(msg);
                }
            }

            if (notice instanceof StateNotification) {
                // debug
                StateNotification tmp = (StateNotification)notice;
                String debugMsg = "StateNotification: ";
                logger.info(msgPrefix + tmp);
                logger.info(msgPrefix + tmp.getReason());
                logger.info(msgPrefix + tmp.getReason().trim());
                if (tmp != null)
                    debugMsg+= tmp.getToState();
                if (m_taskSequence != null && m_taskSequence.getCompletionEvent() != null)
                    debugMsg += "\n inputAtCompletion: " + m_taskSequence.getCompletionEvent().toString();
                logger.debug(msgPrefix + debugMsg);

                // prepare state notification
                StateNotification sn = (StateNotification)notice;
                String toState       = sn.getToState();
                QualifiedResource qr = findQRFromSN(sn);

                if (toState == null ) {
                    logger.warn(msgPrefix + "Received StateNotification with toState==null.");
                    return;
                }

                if (toState.equals(GEMStates.ERROR.toString()) ||
                    toState.equals(GEMStates.FAILED.toString())) {// ||
                    // toState.equals(RCMSConstants.LEASE_RENEWAL_FAILED)) {
                    // fire event to go to error
                    fail("Received " + toState + " notification from " + qr.getURI() + "\n" +
                          "Reason given: " + sn.getReason() );
                    return;
                }
                if (m_taskSequence == null) {
                    String msg = "Received an unexpected StateNotification while taskSequence is null\n"
                        + printStateChangedNotice(sn);
                    logger.warn(msgPrefix + msg);
                    return;
                }

                // // REQUIRES TCDS REIMPLEMENTATION OF Task.java, WHICH IS AWFUL FORM AND SHOULD JUST BE PUT BACK INTO THE MAINLINE
                // // if there is an active task, send state notification to it
                // if( m_activeTask != null ) {
                //     m_activeTask.processStateNotification(qr, toState);
                // }
            }

            // logger.info(msgPrefix + "m_taskSequence.size(): " + m_taskSequence.size());
            if (m_taskSequence == null) {
                setTimeoutThread(false);
                String infomsg = "Received a State Notification while taskSequence is null\n";

                logger.debug(msgPrefix + "FM is in local mode");
                // m_gemFM.m_gemEventHandler.computeNewState(notification);
                return;
            }

            // do a while loop to cover synchronous tasks which finish immediately
            while (m_activeTask == null || m_activeTask.isCompleted()) {
                if (m_activeTask != null) {
                    String msg = "m_activeTask: " + m_activeTask + " completed.";
                    logger.info(msgPrefix + msg);
                }

                if (m_taskSequence.isEmpty()) {
                    String msg = "TaskSequence is empty, tasks may have completed";
                    logger.warn(msgPrefix + msg);
                    m_gemFM.setAction(msg);
                    try {
                        completeTransition();
                    } catch (Exception e) {
                        m_taskSequence = null;
                        msg = "Exception while completing TaskSequence ["
                            + m_taskSequence.getDescription() + "]: " + e.getMessage();
                        logger.error(msgPrefix + msg);
                        m_gemFM.setAction(msg);
                    }
                    break;
                } else {
                    m_activeTask = (Task)m_taskSequence.removeFirst();
                    logger.info(msgPrefix + "Start next task: " + m_activeTask.getDescription());
                    m_gemFM.setAction("Executing: " + m_activeTask.getDescription());
                    try {
                        m_activeTask.startExecution();
                        logger.info(msgPrefix + "After TaskSequence::startExecution(): " + m_taskSequence.completion());
                    } catch (Exception e) {
                        m_taskSequence = null;
                        String msg = "Exception while moving to the next task: " + e.getMessage();
                        handleError(msg,msg);
                    }
                }
            }
        } catch (UserActionException e) {
            logger.error(msgPrefix + "Caught UserActionException during processing occured.", e);
            m_gemFM.fireEvent(GEMInputs.SETERROR);
        }
    }


    // BEGIN COPIED FROM TCDS CODE, NEEDS TO BE RETHOUGHT
    private void fail(String msg)
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMStateNotificationHandler::fail(): ";

        logger.error(msgPrefix + msg);

        ((FunctionManagerParameter<StringT>)m_gemFM.getParameterSet().get(GEMParameters.ERROR_MSG))
            .setValue( new StringT( escapeErrorMessage(msg) ));

        m_gemFM.fireEvent(GEMInputs.SETERROR);
    }


    private String escapeErrorMessage(String msg)
    {
        return msg.replace("<","&lt;").replace(">", "&gt;").replace("\n", "<br/>");
    }


    private QualifiedResource findQRFromSN(StateNotification sn)
    {
        try {
            return m_gemFM.getQualifiedGroup().seekQualifiedResourceOfURI( new URI(sn.getIdentifier()) );
        } catch (URISyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }


    private String printStateChangedNotice(StateNotification notice)
    {
        return "StateUpdate " + "\n Destination = " + notice.getDestination()
            + "\n Identifier  = " + notice.getIdentifier()
            + "\n fromState   = " + notice.getFromState()
            + "\n reason      = " + notice.getReason()
            + "\n toState     = " + notice.getToState();
    }
    // END COPIED FROM TCDS CODE

    protected void executeTaskSequence(TaskSequence taskSequence)
    // throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMStateNotificationHandler::executeTaskSequence(): ";

        this.m_taskSequence = taskSequence;

        State SequenceState = m_taskSequence.getState();
        State FMState       = m_gemFM.getState();

        // Make sure that task list belongs to active state we are in
        if (!SequenceState.equals(FMState)) {
            String msg = "taskSequence does not belong to this state \n "
                + "Function Manager state = " + FMState + "\n"
                + "taskSequence is for state = " + SequenceState;
            logger.error(msg);
            m_taskSequence = null;
            handleError(msg," ");
            return;
            // throw new UserActionException(msg);
        }

        try {
            logger.info(msgPrefix + "Starting execution of TaskSequence: " + m_taskSequence.completion());
            m_taskSequence.startExecution();
            logger.info(msgPrefix + "After m_taskSequence.startExecution(): " + m_taskSequence.completion());
            // } catch (EventHandlerException e) {
        } catch (Exception e) {
            m_taskSequence = null;
            String msg = e.getMessage();
            handleError(msg, " ");
            // throw new UserActionException("Could not start execution of " + m_taskSequence.getDescription(), e);
        }
    }


    protected void completeTransition()
        throws UserActionException, Exception
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMStateNotificationHandler::completeTransition(): ";

        State FMState = m_gemFM.getState();

        m_gemFM.setAction("Transition Completed");

        //fm.setTransitionEndTime();
        setTimeoutThread(false);
        logger.info(msgPrefix + "Fire TaskSequence::getCompletionEvent "
                    + m_taskSequence.getCompletionEvent().toString());
        m_gemFM.fireEvent(m_taskSequence.getCompletionEvent());
        m_activeTask   = null;
        m_taskSequence = null;
    }


    public void setTimeoutThread(Boolean action)
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMStateNotificationHandler::setTimeoutThread(): ";

        setTimeoutThread(action,240000);
    }


    public void setTimeoutThread(Boolean action, int msTimeout)
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMStateNotificationHandler::setTimeoutThread(): ";

    }


    protected void handleError(String errMsg, String actionMsg)
    // throws
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMStateNotificationHandler::handleError(): ";

        m_gemFM.setAction(actionMsg);
        setTimeoutThread(false);
        m_gemFM.goToError(errMsg);
    }
}
