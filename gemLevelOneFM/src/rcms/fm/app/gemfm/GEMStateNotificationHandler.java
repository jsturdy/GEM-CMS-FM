package rcms.fm.app.gemfm;

import rcms.fm.fw.parameter.FunctionManagerParameter;
import rcms.fm.fw.parameter.type.StringT;

import rcms.fm.fw.user.UserActionException;
import rcms.fm.fw.user.UserEventHandler;

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
        subscribeForEvents(StateNotification.class);
        addAnyStateAction("processNotice");
    }


    public void init()
        throws rcms.fm.fw.EventHandlerException
    {
        m_gemFM = (GEMFunctionManager) getUserFunctionManager();
    }

    // State notification callback
    public void processNotice(Object notice)
        throws UserActionException
    {
        StateNotification notification = (StateNotification)notice;
        String            actualState  = m_gemFM.getState().getStateString();

        if (m_gemFM.getState().equals(GEMStates.ERROR)) {
            String msg = "[GEM FM::" + m_gemFM.m_FMname + "]  is in error state: " + m_gemFM.getState();
            logger.warn(msg);
            return;
        }


        // logger.info("[GEM FM::" + m_gemFM.m_FMname + "]  m_taskSequence.size(): " + m_taskSequence.size());

        // do a while loop to cover synchronous tasks which finish immediately
        while (m_activeTask == null || m_activeTask.isCompleted()) {
            if (m_activeTask != null) {
                String msg = "[GEM FM::" + m_gemFM.m_FMname + "]  m_activeTask: " + m_activeTask + " completed.";
                logger.info(msg);
            }
            
            if (m_taskSequence.isEmpty()) {
                String msg = "TaskSequence is empty, tasks may have completed";
                logger.warn("[GEM FM::" + m_gemFM.m_FMname + "] " + msg);
                m_gemFM.setAction(msg);
                try {
                    completeTransition();
                } catch (Exception e) {
                    m_taskSequence = null;
                    msg = "Exception while completing taskSequence ["
                        + m_taskSequence.getDescription() + "]: " + e.getMessage();
                    logger.error("[GEM FM::" + m_gemFM.m_FMname + "] " + msg);
                    m_gemFM.setAction(msg);
                }
                break;
            } else {
                m_activeTask = (Task)m_taskSequence.removeFirst();
                logger.info("[GEM FM::" + m_gemFM.m_FMname + "] Start next task: " + m_activeTask.getDescription());
                m_gemFM.setAction("Executing: " + m_activeTask.getDescription());
                try {
                    m_activeTask.startExecution();
                } catch (Exception e) {
                    m_taskSequence = null;
                    String msg = "Exception while moving to the next task: " + e.getMessage();
                    handleError(msg,msg);
                }
            }
        }
    }


    protected void executeTaskSequence(TaskSequence taskSequence)
    // throws UserActionException
    {
        this.m_taskSequence = taskSequence;

        State SequenceState = taskSequence.getState();
        State FMState       = m_gemFM.getState();

        // Make sure that task list belongs to active state we are in
        if (!SequenceState.equals(FMState)) {
            String msg = "taskList does not belong to this state \n "
                + "Function Manager state = " + FMState + "\n"
                + "taskList is for state = " + SequenceState;
            logger.error(msg);
            taskSequence = null;
            handleError(msg," ");
            return;
            // throw new UserActionException(msg);
        }

        try {
            logger.info("[GEM FM::" + m_gemFM.m_FMname + "]  before taskSequence.completion(): " + taskSequence.completion());
            taskSequence.startExecution();
            logger.info("[GEM FM::" + m_gemFM.m_FMname + "]  after taskSequence.completion(): " + taskSequence.completion());
            // } catch (EventHandlerException e) {
            // } catch (EventHandlerException e) {
        } catch (Exception e) {
            m_taskSequence = null;
            String msg = e.getMessage();
            handleError(msg, " ");
            // throw new UserActionException("Could not start execution of " + taskSequence.getDescription(), e);
        }
    }


    protected void completeTransition()
        throws UserActionException, Exception
    {
        State FMState = m_gemFM.getState();

        m_gemFM.setAction("Transition Completed");

        //fm.setTransitionEndTime();
        setTimeoutThread(false);
        logger.info("[GEM FM::" + m_gemFM.m_FMname + "] completeTransition: fire taskSequence completion event "
                    + m_taskSequence.getCompletionEvent().toString());
        m_gemFM.fireEvent(m_taskSequence.getCompletionEvent());
        m_activeTask   = null;
        m_taskSequence = null;
    }


    public void setTimeoutThread(Boolean action)
    {
        setTimeoutThread(action,240000);
    }


    public void setTimeoutThread(Boolean action, int msTimeout)
    {
    }


    protected void handleError(String errMsg, String actionMsg)
        // throws 
    {
        m_gemFM.setAction(actionMsg);
        setTimeoutThread(false);
        m_gemFM.goToError(errMsg);
    }
}
