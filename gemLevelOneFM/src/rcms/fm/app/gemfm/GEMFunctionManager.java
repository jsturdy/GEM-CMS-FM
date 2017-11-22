package rcms.fm.app.gemfm;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.TimeZone;

import java.net.URI;
import java.net.URL;
import java.net.MalformedURLException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import rcms.fm.fw.EventHandlerException;

import rcms.fm.fw.parameter.FunctionManagerParameter;
import rcms.fm.fw.parameter.CommandParameter;
import rcms.fm.fw.parameter.ParameterSet;
import rcms.fm.fw.parameter.type.IntegerT;
import rcms.fm.fw.parameter.type.StringT;
import rcms.fm.fw.parameter.type.DoubleT;
import rcms.fm.fw.parameter.type.VectorT;
import rcms.fm.fw.parameter.type.BooleanT;

import rcms.fm.fw.user.UserActionException;
import rcms.fm.fw.user.UserFunctionManager;

import rcms.fm.resource.CommandException;
import rcms.fm.resource.QualifiedGroup;
import rcms.fm.resource.QualifiedResource;
import rcms.fm.resource.QualifiedResourceContainer;
import rcms.fm.resource.QualifiedResourceContainerException;
import rcms.fm.resource.StateVector;
import rcms.fm.resource.StateVectorCalculation;

//XDAQ from qualified source and others
import rcms.fm.resource.qualifiedresource.XdaqApplicationContainer;
import rcms.fm.resource.qualifiedresource.XdaqApplication;
import rcms.fm.resource.qualifiedresource.XdaqExecutive;
import rcms.fm.resource.qualifiedresource.FunctionManager;

import rcms.xdaqctl.XDAQParameter;
import rcms.xdaqctl.XDAQMessage;

import net.hep.cms.xdaqctl.XDAQException;
import net.hep.cms.xdaqctl.XDAQTimeoutException;
import net.hep.cms.xdaqctl.XDAQMessageException;


import rcms.resourceservice.db.resource.Resource;
import rcms.resourceservice.db.resource.xdaq.XdaqApplicationResource;
import rcms.resourceservice.db.resource.xdaq.XdaqExecutiveResource;

import net.hep.cms.xdaqctl.WSESubscription; // what is this for?

import rcms.stateFormat.StateNotification;

import rcms.statemachine.definition.Input;
import rcms.statemachine.definition.State;
import rcms.statemachine.definition.StateMachineDefinitionException;

import rcms.resourceservice.db.resource.fm.FunctionManagerResource;

import rcms.util.logger.RCMSLogger;

import rcms.errorFormat.CMS.CMSError;

import rcms.util.logsession.LogSessionException;
import rcms.util.logsession.LogSessionConnector;
import rcms.errorFormat.CMS.CMSError;

import rcms.utilities.fm.task.Task;
import rcms.utilities.fm.task.SimpleTask;
import rcms.utilities.fm.task.TaskSequence;

import rcms.utilities.runinfo.RunInfo;

/**
 * GEM Level 1 Function Manager.
 *
 * @author Andrea Petrucci, Alexander Oh, Michele Gulmini
 * @maintainer Jose Ruiz, Jared Sturdy
 */

public class GEMFunctionManager extends UserFunctionManager {

    /**
     * <code>RCMSLogger</code>: RCMS log4j Logger
     */
    static RCMSLogger logger = new RCMSLogger(GEMFunctionManager.class);

    /**
     * <code>m_gemQC</code>: QualifiedGroup of the GEM FM
     */
    private QualifiedGroup m_gemQG = null;

    /**
     * <code>logSessionConnector</code>: Connector for logsession DB
     */
    public LogSessionConnector logSessionConnector;

    /**
     * define some containers
     */
    public XdaqApplicationContainer c_xdaqApps        = null;  ///<
    public XdaqApplicationContainer c_xdaqServiceApps = null;  ///<

    /**
     * define GEM specific application containers
     */
    public XdaqApplicationContainer c_gemSupervisors  = null;  ///<
    public XdaqApplicationContainer c_amc13Managers   = null;  ///<

    /**
     * define TCDS specific application containers
     */
    public XdaqApplicationContainer c_tcdsControllers = null;  ///<
    public XdaqApplicationContainer c_iciControllers  = null;  ///<
    public XdaqApplicationContainer c_piControllers   = null;  ///<
    public XdaqApplicationContainer c_lpmControllers  = null;  ///<
    public XdaqApplicationContainer c_TTCciControl    = null;  ///< don't probably need this

    /**
     * define FEDKIT/readout specific application containers
     */
    public XdaqApplicationContainer c_uFEDKIT         = null;  ///<
    public XdaqApplicationContainer c_BUs             = null;  ///<
    public XdaqApplicationContainer c_RUs             = null;  ///<
    public XdaqApplicationContainer c_EVMs            = null;  ///<
    public XdaqApplicationContainer c_Ferols          = null;  ///<
    public XdaqApplicationContainer c_FEDStreamer     = null;  ///< don't probably need this

    /**
     * <code>c_xdaqExecs</code>: container of XdaqExecutive in the running Group.
     */
    public XdaqApplicationContainer c_xdaqExecs = null;

    /**
     * <code>c_FMs</code>: container of FunctionManagers in the running Group.
     */
    public QualifiedResourceContainer c_FMs = null;

    /**
     * <code>c_JCs</code>: container of JobControl in the running Group.
     */
    public QualifiedResourceContainer c_JCs = null;

    /**
     * <code>svCalc</code>: Composite state calculator
     */
    public StateVectorCalculation m_svCalc = null;

    /**
     * <code>m_calcState</code>: Calculated State.
     */
    public State m_calcState = null;

    /**
     * <code>degraded</code>: FM is in degraded state
     */
    boolean degraded = false;

    /**
     * <code>softErrorDetected</code>: FM has detected softError
     */
    boolean softErrorDetected = false;

    // connector to the RunInfo database
    public RunInfo GEMRunInfo = null;

    // set from the controlled EventHandler
    protected GEMEventHandler theEventHandler = null;
    protected GEMErrorHandler theErrorHandler = null;
    public String  RunType = "local";
    public Integer RunNumber = 0;
    public String  FEDEnableMask = "0&0%";
    //public Integer CachedRunNumber = 0;

    // GEM RunInfo namespace, the FM name will be added in the createAction() method
    public String GEM_NS = "CMS.";

    // Seems to be standard for all subsystems to create a wrapper so that TaskSequence actually works,
    // would be great if this were put into the main RCMS code as an available library (extendable if desired)
    public GEMStateNotificationHandler m_stateNotificationHandler = null;

    // string containing details on the setup from where this FM was started (copied from HCAL)
    public String m_rcmsStateListenerURL = "";
    public String RunSetupDetails    = "empty";
    public String m_FMfullpath       = "empty";
    public String m_FMname           = "empty";
    public String m_FMurl            = "empty";
    public String m_FMuri            = "empty";
    public String m_FMrole           = "empty";
    public String m_FMpartition      = "empty";
    public String m_utcFMtimeofstart = "empty";
    public Date   m_FMtimeofstart;

    // TCDS command parameters
    public ParameterSet<CommandParameter> tcdsCmdParameterSet = null;

    /**
     * Instantiates an MyFunctionManager.
     */
    public GEMFunctionManager() {
        //
        // Any State Machine Implementation must provide the framework
        // with some information about itself.
        //

        // make the parameters available
        addParameters();
    }

    /*
     * (non-Javadoc)
     *
     * @see rcms.statemachine.user.UserStateMachine#createAction()
     */
    public void createAction(ParameterSet<CommandParameter> pars)
        throws UserActionException
    {
        //
        // This method is called by the framework when the Function Manager is
        // created.

        String msgPrefix = "[GEM FM] GEMFunctionManager::createAction(ParameterSet<CommandParameter>): ";

        System.out.println(msgPrefix + "createAction called.");
        logger.debug(msgPrefix + "createAction called.");

        m_gemQG = qualifiedGroup;
        // Retrieve the configuration for this Function Manager from the Group
        FunctionManagerResource fmConf = ((FunctionManagerResource) m_gemQG.getGroup().getThisResource());

        m_FMfullpath    = fmConf.getDirectory().getFullPath().toString();
        m_FMname        = fmConf.getName();
        m_FMurl         = fmConf.getSourceURL().toString();
        m_FMuri         = fmConf.getURI().toString();
        m_FMrole        = fmConf.getRole();
        m_FMtimeofstart = new Date();
        DateFormat dateFormatter = new SimpleDateFormat("M/d/yy hh:mm:ss a z");
        dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));;
        m_utcFMtimeofstart = dateFormatter.format(m_FMtimeofstart);

        msgPrefix = "[GEM FM::" + m_FMname + "] GEMFunctionManager::createAction(ParameterSet<CommandParameter>): ";

        // set statelistener URL
        try {
            URL fmURL = new URL(m_FMurl);
            String rcmsStateListenerHost     = fmURL.getHost();
            int    rcmsStateListenerPort     = fmURL.getPort()+1;
            String rcmsStateListenerProtocol = fmURL.getProtocol();
            m_rcmsStateListenerURL = rcmsStateListenerProtocol+"://"+rcmsStateListenerHost+":"+rcmsStateListenerPort+"/rcms";
        } catch (MalformedURLException e) {
            String msg = "Caught MalformedURLException";
            logger.error(msgPrefix + msg, e);
            sendCMSError(msg);
            getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
            getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
            // if (theEventHandler.TestMode.equals("off")) { firePriorityEvent(GEMInputs.SETERROR); ErrorState = true; return;}
        }

        // get log session connector
        logger.info(msgPrefix + "Get log session connector started");
        logSessionConnector = getLogSessionConnector();
        logger.info(msgPrefix + "Get log session connector finished");

	getParameterSet().get(GEMParameters.RUN_TYPE).setValue(new StringT(RunType));
        // get session ID // NEEDS TO BE REMOVED FOR GLOBAL OPERATIONS
	if (RunType.equals("local")) {
	    logger.info(msgPrefix + "Starting SID fetching for run type:" + RunType);
	    logger.info(msgPrefix + "Get session ID started");
	    getSessionId();
	    logger.info(msgPrefix + "Get session ID finished");

	}
	else {
	    logger.info(msgPrefix + "No need of fetching SID for run type:" + RunType);
	    logger.warn("[GEM] logSessionConnector = " + logSessionConnector + ", using default = " + getParameterSet().get(GEMParameters.SID).getValue() + ".");
	}

        logger.debug(msgPrefix + "createAction executed.");
    }

    /*
     * (non-Javadoc)
     *
     * @see rcms.statemachine.user.UserStateMachine#destroyAction()
     */
    public void destroyAction()
        throws UserActionException
    {
        //
        // This method is called by the framework when the Function Manager is
        // destroyed.
        //
        //m_gemQG.destroy();

        String msgPrefix = "[GEM FM::" + m_FMname + "] GEMFunctionManager::destroyAction(): ";

        System.out.println(msgPrefix + "destroyAction called");
        logger.debug(msgPrefix + "destroyAction called");

        // try to close any open session ID only if we are in local run mode i.e. not CDAQ and not miniDAQ runs and if it's a LV1FM
        if (RunType.equals("local")) {
            closeSessionId();
        }
        //closeSessionId();  // NEEDS TO BE CORRECTED TO ONLY BE CALLED IN LOCAL RUNS

	//Stop watchthreads before destroying
	//theEventHandler.stopMonitorThread = true;
	theEventHandler.stopGEMSupervisorWatchThread = true;

        try {
            // retrieve the Function Managers and kill themDestroy all XDAQ applications
            destroyXDAQ();
        } catch (UserActionException e){
            String msg = "[GEM FM::" + m_FMname + " ] destroyAction: Got an exception during destroyXDAQ()";
            logger.error(msg + ": " + e);
            goToError(msg,e);
            throw e;
        }

        // make sure we send HALT to all TCDS applications
        if (c_tcdsControllers != null){
            if (!c_tcdsControllers.isEmpty()) {
                try {
                    logger.info(msgPrefix + "Trying to halt TCDS on destroy.");
                    haltTCDSControllers();
                } catch (UserActionException e) {
                    String msg = "got an exception while halting TCDS";
                    logger.error(msgPrefix + msg, e);
                    goToError(msg, e);
                }
            }
        }
        // } catch (UserActionException e){
        //     String errMsg = "[GEM FM::" + m_FMname + " ] Got an exception during destroyAction():";
        //     goToError(errMsg,e);
        //     throw e;
        // }

        String msg = "destroying the Qualified Group";
        logger.info(msgPrefix + msg);
        this.getQualifiedGroup().destroy();

        System.out.println(msgPrefix + "destroyAction executed");
        logger.debug(msgPrefix + "destroyAction executed");
    }

    /**
     * add parameters to parameterSet. After this they are accessible.
     */
    private void addParameters()
    {
        parameterSet        = GEMParameters.LVL_ONE_PARAMETER_SET;
        tcdsCmdParameterSet = GEMParameters.TCDS_PARAMETER_SET;
    }

    public void init()
        throws StateMachineDefinitionException,
               rcms.fm.fw.EventHandlerException
    {
        String msgPrefix = "[GEM FM::" + m_FMname + "] GEMFunctionManager::init(): ";

        // Set first of all the State Machine Definition
        logger.info(msgPrefix + "Setting the state machine definition");
        setStateMachineDefinition(new GEMStateMachineDefinition());

        // Add event handler
        logger.info(msgPrefix + "Adding the GEMEventHandler");
	theEventHandler = new GEMEventHandler();
	addEventHandler(theEventHandler);
        //addEventHandler(new GEMEventHandler());

        // Add error handler
        logger.info(msgPrefix + "Adding the GEMErrorHandler");
	theErrorHandler = new GEMErrorHandler();
	addEventHandler(theErrorHandler);
        //addEventHandler(new GEMErrorHandler());

        // Add state notification handler
        logger.info(msgPrefix + "Creating the state notification handler");
        m_stateNotificationHandler = new GEMStateNotificationHandler();
        logger.info(msgPrefix + "Adding state notification handler");
        addEventHandler(m_stateNotificationHandler);
    }

    // get a session Id
    @SuppressWarnings("unchecked") // SHOULD REALLY MAKE SURE THAT THIS IS NECESSARY AND NOT JUST DUE TO BAD JAVA
        protected void getSessionId()
    {
        String msgPrefix = "[GEM FM::" + m_FMname + "] GEMFunctionManager::getSessionId(): ";

        String user        = getQualifiedGroup().getGroup().getDirectory().getUser();
        String description = getQualifiedGroup().getGroup().getDirectory().getFullPath();
        int sessionId      = 0;

        logger.debug(msgPrefix + "Log session connector: " + logSessionConnector );

        if (logSessionConnector != null) {
            try {
                sessionId = logSessionConnector.createSession( user, description );
                logger.info(msgPrefix + "New session Id obtained =" + sessionId );
            } catch (LogSessionException e1) {
                logger.warn(msgPrefix + "Could not get session ID, using default = " + sessionId + ".\n"
                            + "Exception: ", e1);
            }
        } else {
            logger.warn("[GEM base] logSessionConnector = " + logSessionConnector + ", using default = " + sessionId + ".");
        }

        // put the session ID into parameter set
        logger.info("[GEM base] setting SID to " + sessionId);
        getParameterSet().get(GEMParameters.SID).setValue(new IntegerT(sessionId));
        logger.info("[GEM base] parameter value of SID is "
                    + getParameterSet().get(GEMParameters.SID).getValue());
    }

    // close session Id. This routine is called always when functionmanager gets destroyed.
    protected void closeSessionId()
    {
        String msgPrefix = "[GEM FM::" + m_FMname + "] GEMFunctionManager::closeSessionId(): ";

        if (logSessionConnector != null) {
            int sessionId = 0;
            try {
                sessionId = ((IntegerT)getParameterSet().get(GEMParameters.SID).getValue()).getInteger();
            } catch (Exception e) {
                logger.warn(msgPrefix + "Could not get sessionId for closing session.\n"
                            + "Not closing session.\n"
                            + "(This is OK if no sessionId was requested from within GEM land, i.e. global runs)."
                            + "Exception: ", e);
            }
            try {
                logger.debug(msgPrefix + "Trying to close log sessionId = " + sessionId );
                logSessionConnector.closeSession(sessionId);
                logger.debug(msgPrefix + "Closed log sessionId = " + sessionId );
            } catch (LogSessionException e) {
                logger.warn(msgPrefix + "Could not close sessionId, but sessionId was requested and used.\n"
                            + "This is OK only for global runs.\n"
                            + "Exception: ", e);
            }
        } else {
            logger.warn("[GEM base] logSessionConnector null");
        }
    }

    public boolean isDegraded()
    {
        // FM may check whether it is currently degraded if such functionality exists
        String msgPrefix = "[GEM FM::" + m_FMname + "] GEMFunctionManager::isDegraded(): ";
        return degraded;
    }

    public boolean hasSoftError()
    {
        // FM may check whether the system has a soft error if such functionality exists
        String msgPrefix = "[GEM FM::" + m_FMname + "] GEMFunctionManager::hasSoftError(): ";
        return softErrorDetected;
    }

    // only needed if FM cannot check for degradation
    public void setDegraded(boolean degraded)
    {
        String msgPrefix = "[GEM FM::" + m_FMname + "] GEMFunctionManager::setDegraded(boolean): ";
        this.degraded = degraded;
    }

    // only needed if FM cannot check for softError
    public void setSoftErrorDetected(boolean softErrorDetected)
    {
        String msgPrefix = "[GEM FM::" + m_FMname + "] GEMFunctionManager::setSoftErrorDetected(boolean): ";
        this.softErrorDetected = softErrorDetected;
    }

    @SuppressWarnings("unchecked")
        protected void sendCMSError(String errMessage)
    {
        String msgPrefix = "[GEM FM::" + m_FMname + "] GEMFunctionManager::sendCMSError(String): ";

        // create a new error notification msg
        CMSError error = getErrorFactory().getCMSError();
        error.setDateTime(new Date().toString());
        error.setMessage(errMessage);

        // update error msg parameter for GUI
        getParameterSet().get("ERROR_MSG").setValue(new StringT(errMessage));

        // send error
        /*try {
            getParentErrorNotifier().sendError(error);
        } catch (Exception e) {
            logger.warn(msgPrefix + "" + getClass().toString() + ": Failed to send error message " + errMessage);
	    }*/
	try {
	    theErrorHandler.setError(error);
        } catch (Exception e) {
            logger.warn(msgPrefix + getClass().toString() + ": Failed to send error message " + errMessage);
	}
    }


    /**----------------------------------------------------------------------
     * set the current Action
     */
    public void setAction(String action) {

        String msgPrefix = "[GEM FM::" + m_FMname + "] GEMFunctionManager::setAction(String): ";

        getParameterSet().put(new FunctionManagerParameter<StringT>
                              ("ACTION_MSG",new StringT(action)));
        return;
    }

    /**----------------------------------------------------------------------
     * go to the error state, setting messages and so forth, with exception
     */
    public void goToError(String errMessage, Exception e)
    {
        String msgPrefix = "[GEM FM::" + m_FMname + "] GEMFunctionManager::goToError(String, Exception): ";
        errMessage+= ": Message from the caught exception is: " + e.getMessage();
        goToError(errMessage);
    }

    /**----------------------------------------------------------------------
     * go to the error state, setting messages and so forth, without exception
     */
    public void goToError(String errMessage)
    {
        String msgPrefix = "[GEM FM::" + m_FMname + "] GEMFunctionManager::goToError(String): ";

        sendCMSError(errMessage);
        getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
        getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(errMessage)));
        getParameterSet().put(new FunctionManagerParameter<StringT>("SUPERVISOR_ERROR",new StringT(errMessage)));
        Input errInput = new Input(GEMInputs.SETERROR);
        errInput.setReason(errMessage);
        // if (theEventHandler.TestMode.equals("off")) { firePriorityEvent(errInput); ErrorState = true; }
    }

    /**----------------------------------------------------------------------
     * Get the sequence for TCDS tasks
     */
    // public TaskSequence getTCDSTaskSequence(Input input)
    public void getTCDSTaskSequence(Input input)
        throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_FMname + "] GEMFunctionManager::getTCDSTaskSequence(Input): ";

        TaskSequence tcdsSequence = null;
        // Input is INITIALIZE/HALT
        // LPM then iCI, then PI

        // Input is CONFIGURE/START
        // LPM, then PI, then iCI

        // Input is START/RESUME
        // LPM, then PI then iCI

        // Input is STOP/PAUSE
        // LPM, then iCI, then PI

        // Input is RESET

        // return tcdsSequence;
    }

    /**----------------------------------------------------------------------
     * Get command parameters for TCDS applications
     */
    // public Map<String, ParameterSet<CommandParameter> > getTCDSCommandParameters(Input input)
    public void getTCDSCommandParameters(Input input)
        throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_FMname + "] GEMFunctionManager::getTCDSCommandParameters(Input): ";
        Map<String, ParameterSet<CommandParameter> > tcdsParameters = null;
        // Input is INITIALIZE
        // Input is HALT
        // Input is CONFIGURE
        // Input is START
        // Input is STOP
        // Input is PAUSE
        // Input is RESUME
        // Input is RESET

        // return tcdsParameters;
    }

    public TaskSequence getInitSequence(TaskSequence initTaskSeq)
    {
        String msgPrefix = "[GEM FM] GEMFunctionManager::getInitSequence(): ";

        // force TCDS HALTED
        if (this.c_lpmControllers != null) {
            if (!this.c_lpmControllers.isEmpty()) {
                Input initInputLPM = new Input(GEMInputs.HALT.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                initInputLPM.setParameters(pSet);
                SimpleTask lpmInitTask = new SimpleTask(this.c_lpmControllers,initInputLPM,
                                                        GEMStates.INITIALIZING,GEMStates.HALTED,
                                                        "Initializing LPMControllers");
                initTaskSeq.addLast(lpmInitTask);
            }
        }
        if (this.c_iciControllers != null) {
            if (!this.c_iciControllers.isEmpty()) {
                Input initInputICI = new Input(GEMInputs.HALT.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                initInputICI.setParameters(pSet);
                SimpleTask iciInitTask = new SimpleTask(this.c_iciControllers,initInputICI,
                                                        GEMStates.INITIALIZING,GEMStates.HALTED,
                                                            "Initializing ICIControllers");
                initTaskSeq.addLast(iciInitTask);
            }
        }
        if (this.c_piControllers != null) {
            if (!this.c_piControllers.isEmpty()) {
                Input initInputPI  = new Input(GEMInputs.HALT.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                initInputPI.setParameters(pSet);
                SimpleTask piInitTask = new SimpleTask(this.c_piControllers,initInputPI,
                                                       GEMStates.INITIALIZING,GEMStates.HALTED,
                                                       "Initializing PIControllers");
                initTaskSeq.addLast(piInitTask);
            }
        }

        // initialize GEMFSMApplications
        if (this.c_gemSupervisors != null) {
            if (!this.c_gemSupervisors.isEmpty()) {
                Input initInputGEMSuper  = new Input(GEMInputs.INITIALIZE.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                initInputGEMSuper.setParameters(pSet);
                SimpleTask gemSuperInitTask = new SimpleTask(this.c_gemSupervisors,initInputGEMSuper,
                                                             GEMStates.INITIALIZING,GEMStates.HALTED,
                                                             "Initializing GEMSupervisor");
                initTaskSeq.addLast(gemSuperInitTask);
            }
        }

        // ? ferol/EVM/BU/RU?
        if (this.c_BUs != null) {
            if (!this.c_BUs.isEmpty()) {
                Input initInputBU  = new Input(GEMInputs.HALT.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                initInputBU.setParameters(pSet);
                SimpleTask buInitTask = new SimpleTask(this.c_BUs,initInputBU,
                                                       GEMStates.INITIALIZING,GEMStates.HALTED,
                                                       "Initializing BUs");
                initTaskSeq.addLast(buInitTask);
            }
        }
        if (this.c_RUs != null) {
            if (!this.c_RUs.isEmpty()) {
                Input initInputRU  = new Input(GEMInputs.HALT.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                initInputRU.setParameters(pSet);
                SimpleTask ruInitTask = new SimpleTask(this.c_RUs,initInputRU,
                                                       GEMStates.INITIALIZING,GEMStates.HALTED,
                                                       "Initializing RUs");
                initTaskSeq.addLast(ruInitTask);
            }
        }
        if (this.c_EVMs != null) {
            if (!this.c_EVMs.isEmpty()) {
                Input initInputEVM  = new Input(GEMInputs.HALT.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                initInputEVM.setParameters(pSet);
                SimpleTask evmInitTask = new SimpleTask(this.c_EVMs,initInputEVM,
                                                        GEMStates.INITIALIZING,GEMStates.HALTED,
                                                        "Initializing EVMs");
                initTaskSeq.addLast(evmInitTask);
            }
        }
        if (this.c_Ferols != null) {
            if (!this.c_Ferols.isEmpty()) {
                Input initInputFerol  = new Input(GEMInputs.HALT.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                initInputFerol.setParameters(pSet);
                SimpleTask ferolInitTask = new SimpleTask(this.c_Ferols,initInputFerol,
                                                          GEMStates.INITIALIZING,GEMStates.HALTED,
                                                          "Initializing Ferols");
                initTaskSeq.addLast(ferolInitTask);
            }
        }
        if (this.c_FEDStreamer != null) {
            if (!this.c_FEDStreamer.isEmpty()) {
                Input initInputFEDStreamer  = new Input(GEMInputs.HALT.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                initInputFEDStreamer.setParameters(pSet);
                SimpleTask fedStreamerInitTask = new SimpleTask(this.c_FEDStreamer,initInputFEDStreamer,
                                                                GEMStates.INITIALIZING,GEMStates.HALTED,
                                                                "Initializing FEDStreamers");
                initTaskSeq.addLast(fedStreamerInitTask);
            }
        }

        logger.info(msgPrefix + "returning initTaskSeq");
        return initTaskSeq;
    }

    public TaskSequence getConfSequence(TaskSequence confTaskSeq)
    {
        String msgPrefix = "[GEM FM] GEMFunctionManager::getConfSequence(): ";

        // configure TCDS (LPM then ICI then PI)
        if (this.c_lpmControllers != null) {
            if (!this.c_lpmControllers.isEmpty()) {
                Input confInputLPM = new Input(GEMInputs.HALT.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                pSet.put(new CommandParameter<StringT>("fedEnableMask",               new StringT("0&0%")));
                // prepare command plus the parameters to send
                confInputLPM.setParameters(pSet);
                SimpleTask lpmConfTask = new SimpleTask(this.c_lpmControllers,confInputLPM,
                                                        GEMStates.CONFIGURING,GEMStates.CONFIGURED,
                                                        "Configuring LPMControllers");
                confTaskSeq.addLast(lpmConfTask);
            }
        }
        if (this.c_piControllers != null) {
            if (!this.c_piControllers.isEmpty()) {
                Input confInputPI  = new Input(GEMInputs.HALT.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                pSet.put(new CommandParameter<BooleanT>("skipPLLReset",               new BooleanT(true)));
                pSet.put(new CommandParameter<BooleanT>("usePrimaryTCDS",             new BooleanT(true)));
                pSet.put(new CommandParameter<StringT>("fedEnableMask",               new StringT("0&0%")));
                // prepare command plus the parameters to send
                confInputPI.setParameters(pSet);
                SimpleTask piConfTask = new SimpleTask(this.c_piControllers,confInputPI,
                                                       GEMStates.CONFIGURING,GEMStates.CONFIGURED,
                                                       "Configuring PIControllers");
                confTaskSeq.addLast(piConfTask);
            }
        }
        if (this.c_iciControllers != null) {
            if (!this.c_iciControllers.isEmpty()) {
                Input confInputICI = new Input(GEMInputs.HALT.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                // prepare command plus the parameters to send
                confInputICI.setParameters(pSet);
                SimpleTask iciConfTask = new SimpleTask(this.c_iciControllers,confInputICI,
                                                        GEMStates.CONFIGURING,GEMStates.CONFIGURED,
                                                        "Configuring ICIControllers");
                confTaskSeq.addLast(iciConfTask);
            }
        }

        // configure GEMFSMApplications
        if (this.c_gemSupervisors != null) {
            if (!this.c_gemSupervisors.isEmpty()) {
                XDAQParameter pam = null;
                // prepare and set for all GEM supervisors the RunType
                for (QualifiedResource qr : this.c_gemSupervisors.getApplications()){
                    try {
                        pam = ((XdaqApplication)qr).getXDAQParameter();
                        pam.select(new String[] {"FEDEnableMask"});
                        pam.get();
                        String fedMask = pam.getValue("FEDEnableMask");
                        logger.info(msgPrefix + "got fedMask " + fedMask + " from the supervisor");
                        pam.setValue("FEDDnableMask",this.FEDEnableMask);
                        logger.info(msgPrefix + "sending FEDEnableMask to the supervisor");
                        pam.send();
                        logger.info(msgPrefix + "sent FEDEnableMask to the supervisor");
                    } catch (XDAQTimeoutException e) {
                        String msg = "Error! XDAQTimeoutException when trying to send the FEDEnableMask to the GEM supervisor\n."
                            + "Perhaps this application is dead!?";
                        logger.error(msgPrefix + msg, e);
                        this.goToError(msg, e);
                    } catch (XDAQException e) {
                        String msg = "Error! XDAQException when trying to send the FEDEnableMask to the GEM supervisor";
                        logger.error(msgPrefix + msg, e);
                        this.goToError(msg, e);
                    }
                }

                Input confInputGEMSuper  = new Input(GEMInputs.CONFIGURE.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                confInputGEMSuper.setParameters(pSet);
                SimpleTask gemSuperConfTask = new SimpleTask(this.c_gemSupervisors,confInputGEMSuper,
                                                             GEMStates.CONFIGURING,GEMStates.CONFIGURED,
                                                             "Configuring GEMSupervisor");
                confTaskSeq.addLast(gemSuperConfTask);
            }
        }

        // configure ferol/EVM/BU/RU?
        // need to send the FED_ENABLE_MASK to the EVM and BU
        // need to configure first the EVM then BU and then the FerolController
        /*
        if (this.c_uFEDKIT != null) {
            if (!this.c_uFEDKIT.isEmpty()) {
                try {
                    logger.info(msgPrefix + "Trying to configure uFEDKIT resources on configure.");
                    this.c_uFEDKIT.execute(GEMInputs.CONFIGURE);
                } catch (QualifiedResourceContainerException e) {
                    String msg = "Caught QualifiedResourceContainerException";
                    logger.error(msgPrefix + msg, e);
                    //this.sendCMSError(msg);
                    m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                    m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                }
            }
        }
        */
        // need to ensure that necessary paramters are sent
        // these applications expect empty command transitions it seems
        // if (this.c_uFEDKIT != null) {
        if (this.c_BUs != null) {
            if (!this.c_BUs.isEmpty()) {
                Input confInputBU  = new Input(GEMInputs.CONFIGURE.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                confInputBU.setParameters(pSet);
                SimpleTask buConfTask = new SimpleTask(this.c_BUs,confInputBU,
                                                       GEMStates.CONFIGURING,GEMStates.CONFIGURED,
                                                       "Configuring BUs");
                confTaskSeq.addLast(buConfTask);
            }
        }
        if (this.c_RUs != null) {
            if (!this.c_RUs.isEmpty()) {
                Input confInputRU  = new Input(GEMInputs.CONFIGURE.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                confInputRU.setParameters(pSet);
                SimpleTask ruConfTask = new SimpleTask(this.c_RUs,confInputRU,
                                                       GEMStates.CONFIGURING,GEMStates.CONFIGURED,
                                                       "Configuring RUs");
                confTaskSeq.addLast(ruConfTask);
            }
        }
        if (this.c_EVMs != null) {
            if (!this.c_EVMs.isEmpty()) {
                Input confInputEVM  = new Input(GEMInputs.CONFIGURE.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                confInputEVM.setParameters(pSet);
                SimpleTask evmConfTask = new SimpleTask(this.c_EVMs,confInputEVM,
                                                        GEMStates.CONFIGURING,GEMStates.CONFIGURED,
                                                        "Configuring EVMs");
                confTaskSeq.addLast(evmConfTask);
            }
        }
        if (this.c_Ferols != null) {
            if (!this.c_Ferols.isEmpty()) {
                Input confInputFerol  = new Input(GEMInputs.CONFIGURE.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                confInputFerol.setParameters(pSet);
                SimpleTask ferolConfTask = new SimpleTask(this.c_Ferols,confInputFerol,
                                                          GEMStates.CONFIGURING,GEMStates.CONFIGURED,
                                                          "Configuring Ferols");
                confTaskSeq.addLast(ferolConfTask);
            }
        }
        if (this.c_FEDStreamer != null) {
            if (!this.c_FEDStreamer.isEmpty()) {
                Input confInputFEDStreamer  = new Input(GEMInputs.CONFIGURE.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                confInputFEDStreamer.setParameters(pSet);
                SimpleTask fedStreamerConfTask = new SimpleTask(this.c_FEDStreamer,confInputFEDStreamer,
                                                                GEMStates.CONFIGURING,GEMStates.CONFIGURED,
                                                                "Configuring FEDStreamers");
                confTaskSeq.addLast(fedStreamerConfTask);
            }
        }

        logger.info(msgPrefix + "returning confTaskSeq");
        return confTaskSeq;
    }


    public TaskSequence getStartSequence(TaskSequence startTaskSeq)
    {
        String msgPrefix = "[GEM FM] GEMFunctionManager::getStartSequence(): ";

        // start GEMFSMApplications
        if (this.c_gemSupervisors != null) {
            if (!this.c_gemSupervisors.isEmpty()) {
                XDAQParameter pam = null;
                // prepare and set for all GEM supervisors the RunType
                for (QualifiedResource qr : this.c_gemSupervisors.getApplications()){
                    try {
                        pam = ((XdaqApplication)qr).getXDAQParameter();
                        pam.select(new String[] {"RunNumber"});
                        pam.get();
                        String superRunNumber = pam.getValue("RunNumber");
                        logger.info(msgPrefix + "got run number " + superRunNumber + " from the supervisor");
                        pam.setValue("RunNumber",this.RunNumber.toString());
                        logger.info(msgPrefix + "sending run number " + this.RunNumber.toString() + " to the supervisor");
                        pam.send();
                        logger.info(msgPrefix + "sent run number to the supervisor");
                    } catch (XDAQTimeoutException e) {
                        String msg = "Error! XDAQTimeoutException when trying to send the FEDEnableMask to the GEM supervisor\n."
                            + "Perhaps this application is dead!?";
                        logger.error(msgPrefix + msg, e);
                        this.goToError(msg, e);
                    } catch (XDAQException e) {
                        String msg = "Error! XDAQException when trying to send the FEDEnableMask to the GEM supervisor";
                        logger.error(msgPrefix + msg, e);
                        this.goToError(msg, e);
                    }
                }

                Input startInputGEMSuper  = new Input(GEMInputs.START.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareStartigurationString", new StringT("")));
                startInputGEMSuper.setParameters(pSet);
                SimpleTask gemSuperStartTask = new SimpleTask(this.c_gemSupervisors,startInputGEMSuper,
                                                             GEMStates.STARTING,GEMStates.RUNNING,
                                                             "Starting GEMSupervisor");
                startTaskSeq.addLast(gemSuperStartTask);
            }
        }

        // need to ensure that necessary paramters are sent
        // these applications expect empty command transitions it seems
        if (this.c_BUs != null) {
            if (!this.c_BUs.isEmpty()) {
                XDAQParameter pam = null;
                // prepare and set the runNumber for all BUs
                for (QualifiedResource qr : this.c_BUs.getApplications()){
                    try {
                        pam = ((XdaqApplication)qr).getXDAQParameter();
                        pam.select(new String[] {"runNumber"});
                        pam.get();
                        String buRunNumber = pam.getValue("runNumber");
                        logger.info(msgPrefix + "Obtained run number from the BU: " + buRunNumber);
                        pam.setValue("runNumber",this.RunNumber.toString());
                        logger.info(msgPrefix + "sending run number " + this.RunNumber.toString() + " to the BU");
                        pam.send();
                    } catch (XDAQTimeoutException e) {
                        String msg = "Error! XDAQTimeoutException when trying to send the run number to the BU\n"
                            + "Perhaps this application is dead!?";
                        logger.error(msgPrefix + msg, e);
                        this.goToError(msg, e);
                    } catch (XDAQException e) {
                        String msg = "Error! XDAQException when trying to send the run number to the BU";
                        logger.error(msgPrefix + msg, e);
                        this.goToError(msg, e);
                    }
                }

                Input startInputBU  = new Input(GEMInputs.ENABLE.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareStartigurationString", new StringT("")));
                startInputBU.setParameters(pSet);
                SimpleTask buStartTask = new SimpleTask(this.c_BUs,startInputBU,
                                                       GEMStates.STARTING,GEMStates.RUNNING,
                                                       "Starting BUs");
                startTaskSeq.addLast(buStartTask);
            }
        }
        if (this.c_RUs != null) {
            if (!this.c_RUs.isEmpty()) {
                Input startInputRU  = new Input(GEMInputs.START.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareStartigurationString", new StringT("")));
                startInputRU.setParameters(pSet);
                SimpleTask ruStartTask = new SimpleTask(this.c_RUs,startInputRU,
                                                       GEMStates.STARTING,GEMStates.RUNNING,
                                                       "Starting RUs");
                startTaskSeq.addLast(ruStartTask);
            }
        }
        if (this.c_EVMs != null) {
            if (!this.c_EVMs.isEmpty()) {
                XDAQParameter pam = null;
                // prepare and set the runNumber for all EVMs
                for (QualifiedResource qr : this.c_EVMs.getApplications()){
                    try {
                        pam = ((XdaqApplication)qr).getXDAQParameter();
                        pam.select(new String[] {"runNumber"});
                        pam.get();
                        String evmRunNumber = pam.getValue("runNumber");
                        logger.info(msgPrefix + "Obtained run number from the EVM: " + evmRunNumber);
                        pam.setValue("runNumber",this.RunNumber.toString());
                        logger.info(msgPrefix + "sending run number " + this.RunNumber.toString() + " to the EVM");
                        pam.send();
                    } catch (XDAQTimeoutException e) {
                        String msg = "Error! XDAQTimeoutException when trying to send the run number to the EVM\n"
                            + "Perhaps this application is dead!?";
                        logger.error(msgPrefix + msg, e);
                        this.goToError(msg, e);
                    } catch (XDAQException e) {
                        String msg = "Error! XDAQException when trying to send the run number to the EVM";
                        logger.error(msgPrefix + msg, e);
                        this.goToError(msg, e);
                    }
                }

                Input startInputEVM  = new Input(GEMInputs.ENABLE.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareStartigurationString", new StringT("")));
                startInputEVM.setParameters(pSet);
                SimpleTask evmStartTask = new SimpleTask(this.c_EVMs,startInputEVM,
                                                        GEMStates.STARTING,GEMStates.RUNNING,
                                                        "Starting EVMs");
                startTaskSeq.addLast(evmStartTask);
            }
        }
        if (this.c_Ferols != null) {
            if (!this.c_Ferols.isEmpty()) {
                Input startInputFerol  = new Input(GEMInputs.ENABLE.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareStartigurationString", new StringT("")));
                startInputFerol.setParameters(pSet);
                SimpleTask ferolStartTask = new SimpleTask(this.c_Ferols,startInputFerol,
                                                          GEMStates.STARTING,GEMStates.RUNNING,
                                                          "Starting Ferols");
                startTaskSeq.addLast(ferolStartTask);
            }
        }
        if (this.c_FEDStreamer != null) {
            if (!this.c_FEDStreamer.isEmpty()) {
                Input startInputFEDStreamer  = new Input(GEMInputs.ENABLE.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareStartigurationString", new StringT("")));
                startInputFEDStreamer.setParameters(pSet);
                SimpleTask fedStreamerStartTask = new SimpleTask(this.c_FEDStreamer,startInputFEDStreamer,
                                                                GEMStates.STARTING,GEMStates.RUNNING,
                                                                "Starting FEDStreamers");
                startTaskSeq.addLast(fedStreamerStartTask);
            }
        }

        // TCDS
        if (this.c_lpmControllers != null) {
            if (!this.c_lpmControllers.isEmpty()) {
                Input startInputLPM = new Input(GEMInputs.ENABLE.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // prepare command plus the parameters to send
                startInputLPM.setParameters(pSet);
                SimpleTask lpmStartTask = new SimpleTask(this.c_lpmControllers,startInputLPM,
                                                         GEMStates.STARTING,GEMStates.RUNNING,
                                                         "Starting LPMControllers");
                startTaskSeq.addLast(lpmStartTask);
            }
        }
        if (this.c_piControllers != null) {
            if (!this.c_piControllers.isEmpty()) {
                Input startInputPI  = new Input(GEMInputs.ENABLE.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // prepare command plus the parameters to send
                startInputPI.setParameters(pSet);
                SimpleTask piStartTask = new SimpleTask(this.c_piControllers,startInputPI,
                                                        GEMStates.STARTING,GEMStates.RUNNING,
                                                        "Starting PIControllers");
                startTaskSeq.addLast(piStartTask);
            }
        }
        if (this.c_iciControllers != null) {
            if (!this.c_iciControllers.isEmpty()) {
                Input startInputICI = new Input(GEMInputs.ENABLE.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // prepare command plus the parameters to send
                startInputICI.setParameters(pSet);
                SimpleTask iciStartTask = new SimpleTask(this.c_iciControllers,startInputICI,
                                                         GEMStates.STARTING,GEMStates.RUNNING,
                                                         "Starting ICIControllers");
                startTaskSeq.addLast(iciStartTask);
            }
        }

        logger.info(msgPrefix + "returning startTaskSeq");
        return startTaskSeq;
    }

    public TaskSequence getPauseSequence(TaskSequence pauseTaskSeq)
    {
        String msgPrefix = "[GEM FM] GEMFunctionManager::getPauseSequence(): ";

        // pause TCDS
        if (this.c_lpmControllers != null) {
            if (!this.c_lpmControllers.isEmpty()) {
                Input pauseInputLPM = new Input(GEMInputs.PAUSE.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                pauseInputLPM.setParameters(pSet);
                SimpleTask lpmPauseTask = new SimpleTask(this.c_lpmControllers,pauseInputLPM,
                                                         GEMStates.PAUSING,GEMStates.PAUSED,
                                                         "Pausing LPMControllers");
                pauseTaskSeq.addLast(lpmPauseTask);
            }
        }
        if (this.c_iciControllers != null) {
            if (!this.c_iciControllers.isEmpty()) {
                Input pauseInputICI = new Input(GEMInputs.PAUSE.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                pauseInputICI.setParameters(pSet);
                SimpleTask iciPauseTask = new SimpleTask(this.c_iciControllers,pauseInputICI,
                                                             GEMStates.PAUSING,GEMStates.PAUSED,
                                                         "Pausing ICIControllers");
                pauseTaskSeq.addLast(iciPauseTask);
            }
        }
        if (this.c_piControllers != null) {
            if (!this.c_piControllers.isEmpty()) {
                Input pauseInputPI  = new Input(GEMInputs.PAUSE.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                pauseInputPI.setParameters(pSet);
                SimpleTask piPauseTask = new SimpleTask(this.c_piControllers,pauseInputPI,
                                                        GEMStates.PAUSING,GEMStates.PAUSED,
                                                        "Pausing PIControllers");
                pauseTaskSeq.addLast(piPauseTask);
            }
        }

        // pause GEMFSMApplications
        if (this.c_gemSupervisors != null) {
            if (!this.c_gemSupervisors.isEmpty()) {
                Input pauseInputGEMSuper = new Input(GEMInputs.PAUSE.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                pauseInputGEMSuper.setParameters(pSet);
                SimpleTask gemSuperPauseTask = new SimpleTask(this.c_gemSupervisors,pauseInputGEMSuper,
                                                              GEMStates.PAUSING,GEMStates.PAUSED,
                                                              "Pausing GEMSupervisor");
                pauseTaskSeq.addLast(gemSuperPauseTask);
            }
        }

        /*
        // PAUSE ferol/EVM/BU/RU?
        if (this.c_uFEDKIT != null) {
            if (!this.c_uFEDKIT.isEmpty()) {
                try {
                    logger.info(msgPrefix + "Trying to pause uFEDKIT resources on pause.");
                    this.c_uFEDKIT.execute(GEMInputs.PAUSE);
                } catch (QualifiedResourceContainerException e) {
                    String msg = "Caught QualifiedResourceContainerException";
                    logger.error(msgPrefix + msg, e);
                    this.goToError(msg, e);
                    // this.sendCMSError(msg);  // when do this rather than goToError, or both?
                    m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                    m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                }
            }
        }
        */

        logger.info(msgPrefix + "returning pauseTaskSeq");
        return pauseTaskSeq;
    }

    public TaskSequence getResumeSequence(TaskSequence resumeTaskSeq)
    {
        String msgPrefix = "[GEM FM] GEMFunctionManager::getResumeSequence(): ";

        if (this.c_gemSupervisors != null) {
            if (!this.c_gemSupervisors.isEmpty()) {
                // new run number during resume?
                /*
                XDAQParameter pam = null;
                // prepare and set for all GEM supervisors the RunType
                for (QualifiedResource qr : this.c_gemSupervisors.getApplications()){
                    try {
                        pam = ((XdaqApplication)qr).getXDAQParameter();
                        pam.select(new String[] {"RunNumber"});
                        pam.get();
                        String superRunNumber = pam.getValue("RunNumber");
                        logger.info(msgPrefix + "Obtained run number from supervisor: " + superRunNumber);
                        pam.setValue("RunNumber",this.RunNumber.toString());
                        logger.info(msgPrefix + "sending run number " + this.RunNumber.toString() + " to the supervisor");
                        pam.send();
                    } catch (XDAQTimeoutException e) {
                        String msg = "Error! XDAQTimeoutException: startAction() when "
                            + " trying to send the run number to the GEM supervisor\n Perhaps this "
                            + "application is dead!?";
                        logger.error(msgPrefix + msg, e);
                        this.goToError(msg, e);
                    } catch (XDAQException e) {
                        String msg = "Error! XDAQException: startAction() when trying "
                            + "to send the run number to the GEM supervisor";
                        logger.error(msgPrefix + msg, e);
                        this.goToError(msg, e);
                    }
                }
                */

                Input resumeInputGEMSuper  = new Input(GEMInputs.RESUME.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                resumeInputGEMSuper.setParameters(pSet);
                SimpleTask gemSuperResumeTask = new SimpleTask(this.c_gemSupervisors,resumeInputGEMSuper,
                                                             GEMStates.RESUMING,GEMStates.RUNNING,
                                                             "Resuming GEMSupervisor");
                resumeTaskSeq.addLast(gemSuperResumeTask);
            }
        }

        /*
        // RESUME? ferol/EVM/BU/RU?
        // need to resume first the EVM then BU and then the FerolController
        if (this.c_uFEDKIT != null) {
            if (!this.c_uFEDKIT.isEmpty()) {
                try {
                    logger.info(msgPrefix + "Trying to enable uFEDKIT resources on start.");
                    this.c_uFEDKIT.execute(GEMInputs.ENABLE);
                } catch (QualifiedResourceContainerException e) {
                    String msg = "Caught QualifiedResourceContainerException";
                    logger.error(msgPrefix + msg, e);
                    this.goToError(msg, e);
                    // this.sendCMSError(msg);  // when do this rather than goToError, or both?
                    m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                    m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                }
            }
        }
        */

        // RESUME TCDS
        if (this.c_tcdsControllers != null) {
            if (!this.c_tcdsControllers.isEmpty()) {
                if (!this.c_lpmControllers.isEmpty()) {
                    Input resumeInputLPM = new Input(GEMInputs.RESUME.toString());
                    ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                    // prepare command plus the parameters to send
                    resumeInputLPM.setParameters(pSet);
                    SimpleTask lpmResumeTask = new SimpleTask(this.c_lpmControllers,resumeInputLPM,
                                                            GEMStates.RESUMING,GEMStates.RUNNING,
                                                            "Resuming LPMControllers");
                    resumeTaskSeq.addLast(lpmResumeTask);
                }
                if (!this.c_piControllers.isEmpty()) {
                    Input resumeInputPI  = new Input(GEMInputs.RESUME.toString());
                    ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                    // prepare command plus the parameters to send
                    resumeInputPI.setParameters(pSet);
                    SimpleTask piResumeTask = new SimpleTask(this.c_piControllers,resumeInputPI,
                                                           GEMStates.RESUMING,GEMStates.RUNNING,
                                                           "Resuming PIControllers");
                    resumeTaskSeq.addLast(piResumeTask);
                }
                if (!this.c_iciControllers.isEmpty()) {
                    Input resumeInputICI = new Input(GEMInputs.RESUME.toString());
                    ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                    // prepare command plus the parameters to send
                    resumeInputICI.setParameters(pSet);
                    SimpleTask iciResumeTask = new SimpleTask(this.c_iciControllers,resumeInputICI,
                                                            GEMStates.RESUMING,GEMStates.RUNNING,
                                                            "Resuming ICIControllers");
                    resumeTaskSeq.addLast(iciResumeTask);
                }
            }
        }

        logger.info(msgPrefix + "returning resumeTaskSeq");
        return resumeTaskSeq;
    }

    public TaskSequence getStopSequence(TaskSequence stopTaskSeq)
    {
        String msgPrefix = "[GEM FM] GEMFunctionManager::getStopSequence(): ";

        // stop TCDS
        if (this.c_lpmControllers != null) {
            if (!this.c_lpmControllers.isEmpty()) {
                Input stopInputLPM = new Input(GEMInputs.STOP.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                stopInputLPM.setParameters(pSet);
                SimpleTask lpmStopTask = new SimpleTask(this.c_lpmControllers,stopInputLPM,
                                                        GEMStates.STOPPING,GEMStates.CONFIGURED,
                                                        "Stopping LPMControllers");
                stopTaskSeq.addLast(lpmStopTask);
            }
        }
        if (this.c_iciControllers != null) {
            if (!this.c_iciControllers.isEmpty()) {
                Input stopInputICI = new Input(GEMInputs.STOP.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                stopInputICI.setParameters(pSet);
                SimpleTask iciStopTask = new SimpleTask(this.c_iciControllers,stopInputICI,
                                                        GEMStates.STOPPING,GEMStates.CONFIGURED,
                                                        "Stopping ICIControllers");
                stopTaskSeq.addLast(iciStopTask);
            }
        }
        if (this.c_piControllers != null) {
            if (!this.c_piControllers.isEmpty()) {
                Input stopInputPI  = new Input(GEMInputs.STOP.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                stopInputPI.setParameters(pSet);
                SimpleTask piStopTask = new SimpleTask(this.c_piControllers,stopInputPI,
                                                       GEMStates.STOPPING,GEMStates.CONFIGURED,
                                                       "Stopping PIControllers");
                stopTaskSeq.addLast(piStopTask);
            }
        }

        // stop GEMFSMApplications
        if (this.c_gemSupervisors != null) {
            if (!this.c_gemSupervisors.isEmpty()) {
                Input stopInputGEMSuper  = new Input(GEMInputs.STOP.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                stopInputGEMSuper.setParameters(pSet);
                SimpleTask gemSuperStopTask = new SimpleTask(this.c_gemSupervisors,stopInputGEMSuper,
                                                             GEMStates.STOPPING,GEMStates.CONFIGURED,
                                                             "Stopping GEMSupervisor");
                stopTaskSeq.addLast(gemSuperStopTask);
            }
        }

        // ? ferol/EVM/BU/RU?
        // stop ferol then BU then EVM
        /*
        if (this.c_uFEDKIT != null) {
            if (!this.c_uFEDKIT.isEmpty()) {
                try {
                    logger.info(msgPrefix + "Trying to stop uFEDKIT resources on stop.");
                    this.c_uFEDKIT.execute(GEMInputs.STOP);
                } catch (QualifiedResourceContainerException e) {
                    String msg = "Caught QualifiedResourceContainerException";
                    logger.error(msgPrefix + msg, e);
                    this.goToError(msg, e);
                    // this.sendCMSError(msg);  // when do this rather than goToError, or both?
                    m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                    m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                }
            }
        }
        */

        if (this.c_EVMs != null) {
            if (!this.c_EVMs.isEmpty()) {
                Input stopInputEVM  = new Input(GEMInputs.STOP.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                stopInputEVM.setParameters(pSet);
                SimpleTask evmStopTask = new SimpleTask(this.c_EVMs,stopInputEVM,
                                                        GEMStates.STOPPING,GEMStates.CONFIGURED,
                                                        "Stopping EVMs");
                stopTaskSeq.addLast(evmStopTask);
            }
        }

        if (this.c_RUs != null) {
            if (!this.c_RUs.isEmpty()) {
                Input stopInputRU  = new Input(GEMInputs.STOP.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                stopInputRU.setParameters(pSet);
                SimpleTask ruStopTask = new SimpleTask(this.c_RUs,stopInputRU,
                                                       GEMStates.STOPPING,GEMStates.CONFIGURED,
                                                       "Stopping RUs");
                stopTaskSeq.addLast(ruStopTask);
            }
        }

        if (this.c_BUs != null) {
            if (!this.c_BUs.isEmpty()) {
                Input stopInputBU  = new Input(GEMInputs.STOP.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                stopInputBU.setParameters(pSet);
                SimpleTask buStopTask = new SimpleTask(this.c_BUs,stopInputBU,
                                                       GEMStates.STOPPING,GEMStates.CONFIGURED,
                                                       "Stopping BUs");
                stopTaskSeq.addLast(buStopTask);
            }
        }

        if (this.c_Ferols != null) {
            if (!this.c_Ferols.isEmpty()) {
                Input stopInputFerol  = new Input(GEMInputs.STOP.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                stopInputFerol.setParameters(pSet);
                SimpleTask ferolStopTask = new SimpleTask(this.c_Ferols,stopInputFerol,
                                                          GEMStates.STOPPING,GEMStates.CONFIGURED,
                                                          "Stopping Ferols");
                stopTaskSeq.addLast(ferolStopTask);
            }
        }

        logger.info(msgPrefix + "returning stopTaskSeq");
        return stopTaskSeq;
    }

    public TaskSequence getHaltSequence(TaskSequence haltTaskSeq)
    {
        String msgPrefix = "[GEM FM] GEMFunctionManager::getHaltSequence(): ";

        // halt TCDS
        if (this.c_lpmControllers != null) {
            if (!this.c_lpmControllers.isEmpty()) {
                Input haltInputLPM = new Input(GEMInputs.HALT.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                haltInputLPM.setParameters(pSet);
                SimpleTask lpmHaltTask = new SimpleTask(this.c_lpmControllers,haltInputLPM,
                                                        GEMStates.HALTING,GEMStates.HALTED,
                                                        "Halting LPMControllers");
                haltTaskSeq.addLast(lpmHaltTask);
            }
        }
        if (this.c_iciControllers != null) {
            if (!this.c_iciControllers.isEmpty()) {
                Input haltInputICI = new Input(GEMInputs.HALT.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                haltInputICI.setParameters(pSet);
                SimpleTask iciHaltTask = new SimpleTask(this.c_iciControllers,haltInputICI,
                                                        GEMStates.HALTING,GEMStates.HALTED,
                                                        "Halting ICIControllers");
                haltTaskSeq.addLast(iciHaltTask);
            }
        }
        if (this.c_piControllers != null) {
            if (!this.c_piControllers.isEmpty()) {
                Input haltInputPI  = new Input(GEMInputs.HALT.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                haltInputPI.setParameters(pSet);
                SimpleTask piHaltTask = new SimpleTask(this.c_piControllers,haltInputPI,
                                                       GEMStates.HALTING,GEMStates.HALTED,
                                                       "Halting PIControllers");
                haltTaskSeq.addLast(piHaltTask);
            }
        }

        // halt GEMFSMApplications
        if (this.c_gemSupervisors != null) {
            if (!this.c_gemSupervisors.isEmpty()) {
                Input haltInputGEMSuper  = new Input(GEMInputs.HALT.toString());
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                haltInputGEMSuper.setParameters(pSet);
                SimpleTask gemSuperHaltTask = new SimpleTask(this.c_gemSupervisors,haltInputGEMSuper,
                                                             GEMStates.HALTING,GEMStates.HALTED,
                                                             "Halting GEMSupervisor");
                haltTaskSeq.addLast(gemSuperHaltTask);
            }
        }

        // ? ferol/EVM/BU/RU?
        if (this.c_uFEDKIT != null) {
            if (!this.c_uFEDKIT.isEmpty()) {
                if (!this.c_BUs.isEmpty()) {
                    Input haltInputBU  = new Input(GEMInputs.HALT.toString());
                    ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                    // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                    haltInputBU.setParameters(pSet);
                    SimpleTask buHaltTask = new SimpleTask(this.c_BUs,haltInputBU,
                                                           GEMStates.HALTING,GEMStates.HALTED,
                                                           "Halting BUs");
                    haltTaskSeq.addLast(buHaltTask);
                }
                if (!this.c_RUs.isEmpty()) {
                    Input haltInputRU  = new Input(GEMInputs.HALT.toString());
                    ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                    // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                    haltInputRU.setParameters(pSet);
                    SimpleTask ruHaltTask = new SimpleTask(this.c_RUs,haltInputRU,
                                                           GEMStates.HALTING,GEMStates.HALTED,
                                                           "Halting RUs");
                    haltTaskSeq.addLast(ruHaltTask);
                }
                if (!this.c_EVMs.isEmpty()) {
                    Input haltInputEVM  = new Input(GEMInputs.HALT.toString());
                    ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                    // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                    haltInputEVM.setParameters(pSet);
                    SimpleTask evmHaltTask = new SimpleTask(this.c_EVMs,haltInputEVM,
                                                            GEMStates.HALTING,GEMStates.HALTED,
                                                            "Halting EVMs");
                    haltTaskSeq.addLast(evmHaltTask);
                }
                if (!this.c_Ferols.isEmpty()) {
                    Input haltInputFerol  = new Input(GEMInputs.HALT.toString());
                    ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                    // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                    haltInputFerol.setParameters(pSet);
                    SimpleTask ferolHaltTask = new SimpleTask(this.c_Ferols,haltInputFerol,
                                                              GEMStates.HALTING,GEMStates.HALTED,
                                                              "Halting Ferols");
                    haltTaskSeq.addLast(ferolHaltTask);
                }
                // if (!this.c_FEDStreamer.isEmpty()) {
                //     Input haltInputFEDStreamer  = new Input(GEMInputs.HALT.toString());
                //     ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                //     // pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                //     haltInputFEDStreamer.setParameters(pSet);
                //     SimpleTask fedStreamerHaltTask = new SimpleTask(this.c_FEDStreamer,haltInputFEDStreamer,
                //                                                     GEMStates.HALTING,GEMStates.HALTED,
                //                                                     "Halting FEDStreamers");
                //     haltTaskSeq.addLast(fedStreamerHaltTask);
                // }
            }
        }

        logger.info(msgPrefix + "returning haltTaskSeq");
        return haltTaskSeq;
    }

    /**----------------------------------------------------------------------
     * halt the TCDS controllers
     */
    public void haltTCDSControllers()
        throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_FMname + "] GEMFunctionManager::haltTCDSControllers(): ";

        // ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
        // int sessionId = ((IntegerT)getParameterSet().get(GEMParameters.SID).getValue()).getInteger();
        // pSet.put(new FunctionManagerParameter<IntegerT>("SID", new IntegerT(sessionId)));

        try {
            if (!c_lpmControllers.isEmpty()) {
                logger.info(msgPrefix + " Sending halt to LPM ");
                c_lpmControllers.execute(GEMInputs.HALT);
                // if LPM is not a service app, need to provide rcmsURL
                //lpmApp.execute(GEMInputs.HALT,"test",m_rcmsStateListenerURL);
            }
            if (!c_iciControllers.isEmpty()) {
                logger.info(msgPrefix + " Sending halt to iCI ");
                c_iciControllers.execute(GEMInputs.HALT);
            }
            if (!c_piControllers.isEmpty()) {
                logger.info(msgPrefix + " Sending halt to PI ");
                c_piControllers.execute(GEMInputs.HALT);
            }
        } catch (QualifiedResourceContainerException e) {
            String msg = " haltTCDSControllers: ";
            Map<QualifiedResource, CommandException> CommandExceptionMap = e.getCommandExceptionMap();
            for (QualifiedResource qr : CommandExceptionMap.keySet()){
                msg += " Failed to halt "+ qr.getName() + " with reason: " +  CommandExceptionMap.get(qr).getFaultString() +"\n";
            }
            throw new UserActionException(msg);
        }
    }

    /**
     * configure the TCDS controllers
     */
    // public TaskSequence configureTCDSControllers()
    public void configureTCDSControllers()
        throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_FMname + "] GEMFunctionManager::configureTCDSControllers(): ";

        try {
            TaskSequence configureTaskSeq = new TaskSequence(GEMStates.CONFIGURING,GEMInputs.SETCONFIGURE);

            Input configureInputLPM = new Input(GEMInputs.CONFIGURE.toString());
            Input configureInputICI = new Input(GEMInputs.CONFIGURE.toString());
            Input configureInputPI  = new Input(GEMInputs.CONFIGURE.toString());
            SimpleTask lpmConfigureTask,piConfigureTask,iciConfigureTask;
            if (!c_lpmControllers.isEmpty()) {
                logger.info(msgPrefix + " Sending configure to LPM ");
                // add LPM configuration string to CommandParameter
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                pSet.put(new CommandParameter<StringT>("fedEnableMask",               new StringT("0&0%")));
                // prepare command plus the parameters to send
                // Input configureInput = new Input(GEMInputs.CONFIGURE.toString());
                configureInputLPM.setParameters( pSet );
                lpmConfigureTask = new SimpleTask(c_lpmControllers,configureInputLPM,
                                                  GEMStates.CONFIGURING,GEMStates.CONFIGURED,
                                                  "Configuring LPMControllers");
                configureTaskSeq.addLast(lpmConfigureTask);
                // c_lpmControllers.execute(GEMInputs.CONFIGURE);
                // if LPM is not a service app, need to provide rcmsURL
                //lpmApp.execute(GEMInputs.CONFIGURE,"test",m_rcmsStateListenerURL);
            }

            if (!c_piControllers.isEmpty()) {
                logger.info(msgPrefix + " Sending configure to PI ");
                // add PI configuration string to CommandParameter
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                pSet.put(new CommandParameter<BooleanT>("skipPLLReset",               new BooleanT(true)));
                pSet.put(new CommandParameter<BooleanT>("usePrimaryTCDS",             new BooleanT(true)));
                pSet.put(new CommandParameter<StringT>("fedEnableMask",               new StringT("0&0%")));
                // prepare command plus the parameters to send
                // Input configureInput = new Input(GEMInputs.CONFIGURE.toString());
                configureInputPI.setParameters( pSet );
                piConfigureTask = new SimpleTask(c_piControllers,configureInputPI,
                                                 GEMStates.CONFIGURING,GEMStates.CONFIGURED,
                                                 "Configuring PIControllers");
                configureTaskSeq.addLast(piConfigureTask);
                // c_piControllers.execute(GEMInputs.CONFIGURE);
            }

            if (!c_iciControllers.isEmpty()) {
                logger.info(msgPrefix + " Sending configure to iCI ");
                // add ICI configuration string to CommandParameter
                ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                pSet.put(new CommandParameter<StringT>("hardwareConfigurationString", new StringT("")));
                // prepare command plus the parameters to send
                // Input configureInput = new Input(GEMInputs.CONFIGURE.toString());
                configureInputICI.setParameters( pSet );
                iciConfigureTask = new SimpleTask(c_iciControllers,configureInputICI,
                                                  GEMStates.CONFIGURING,GEMStates.CONFIGURED,
                                                  "Configuring ICIControllers");
                configureTaskSeq.addLast(iciConfigureTask);
                // c_iciControllers.execute(GEMInputs.CONFIGURE);
            }

            // try {
            //     // submitTaskList(configureTaskSeq);
            // } catch (UserActionException e) {
            //     throw e;
            // }
            this.m_stateNotificationHandler.executeTaskSequence(configureTaskSeq);
            // return configureTaskSeq;
        } catch (Exception e) {
            // } catch (QualifiedResourceContainerException e) {
            String errMsg = " configureTCDSControllers: ";
            // Map<QualifiedResource, CommandException> CommandExceptionMap = e.getCommandExceptionMap();
            // for (QualifiedResource qr : CommandExceptionMap.keySet()){
            //     errMsg += " Failed to configure "+ qr.getName() + " with reason: "
            //         + CommandExceptionMap.get(qr).getFaultString() +"\n";
            // }
            throw new UserActionException(errMsg);
        }
    }

    /**
     * enable the TCDS controllers
     */
    public void enableTCDSControllers()
        throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_FMname + "] GEMFunctionManager::enableTCDSControllers(): ";

        try {
            if (!c_lpmControllers.isEmpty()) {
                logger.info(msgPrefix + " Sending enable to LPM ");
                c_lpmControllers.execute(GEMInputs.ENABLE);
                // if LPM is not a service app, need to provide rcmsURL
                //lpmApp.execute(GEMInputs.ENABLE,"test",m_rcmsStateListenerURL);
            }
            if (!c_piControllers.isEmpty()) {
                logger.info(msgPrefix + " Sending enable to PI ");
                c_piControllers.execute(GEMInputs.ENABLE);
            }
            if (!c_iciControllers.isEmpty()) {
                logger.info(msgPrefix + " Sending enable to iCI ");
                c_iciControllers.execute(GEMInputs.ENABLE);
            }
        } catch (QualifiedResourceContainerException e) {
            String errMsg = " enableTCDSControllers: ";
            Map<QualifiedResource, CommandException> CommandExceptionMap = e.getCommandExceptionMap();
            for (QualifiedResource qr : CommandExceptionMap.keySet()){
                errMsg += " Failed to enable "+ qr.getName() + " with reason: "
                    + CommandExceptionMap.get(qr).getFaultString() +"\n";
            }
            throw new UserActionException(errMsg);
        }
    }

    /**----------------------------------------------------------------------
     * Stop the TCDS controllers
     */
    public void stopTCDSControllers()
        throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_FMname + "] GEMFunctionManager::stopTCDSControllers(): ";

        try {
            if (!c_lpmControllers.isEmpty()) {
                logger.info(msgPrefix + " Sending stop to LPM ");
                c_lpmControllers.execute(GEMInputs.STOP);
                // if LPM is not a service app, need to provide rcmsURL
                //lpmApp.execute(GEMInputs.STOP,"test",m_rcmsStateListenerURL);
            }
            if (!c_iciControllers.isEmpty()) {
                logger.info(msgPrefix + " Sending stop to iCI ");
                c_iciControllers.execute(GEMInputs.STOP);
            }
            if (!c_piControllers.isEmpty()) {
                logger.info(msgPrefix + " Sending stop to PI ");
                c_piControllers.execute(GEMInputs.STOP);
            }
        } catch (QualifiedResourceContainerException e) {
            String errMsg = " stopTCDSControllers: ";
            Map<QualifiedResource, CommandException> CommandExceptionMap = e.getCommandExceptionMap();
            for (QualifiedResource qr : CommandExceptionMap.keySet()){
                errMsg += " Failed to stop "+ qr.getName() + " with reason: "
                    + CommandExceptionMap.get(qr).getFaultString() +"\n";
            }
            throw new UserActionException(errMsg);
        }
    }

    /**----------------------------------------------------------------------
     * Pause the TCDS controllers
     */
    public void pauseTCDSControllers()
        throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_FMname + "] GEMFunctionManager::pauseTCDSControllers(): ";

        try {
            if (!c_lpmControllers.isEmpty()) {
                logger.info(msgPrefix + " Sending pause to LPM ");
                c_lpmControllers.execute(GEMInputs.PAUSE);
                // if LPM is not a service app, need to provide rcmsURL
                //lpmApp.execute(GEMInputs.PAUSE,"test",m_rcmsStateListenerURL);
            }
            if (!c_iciControllers.isEmpty()) {
                logger.info(msgPrefix + " Sending pause to iCI ");
                c_iciControllers.execute(GEMInputs.PAUSE);
            }
            if (!c_piControllers.isEmpty()) {
                logger.info(msgPrefix + " Sending pause to PI ");
                c_piControllers.execute(GEMInputs.PAUSE);
            }
        } catch (QualifiedResourceContainerException e) {
            String errMsg = " pauseTCDSControllers: ";
            Map<QualifiedResource, CommandException> CommandExceptionMap = e.getCommandExceptionMap();
            for (QualifiedResource qr : CommandExceptionMap.keySet()){
                errMsg += " Failed to pause "+ qr.getName() + " with reason: "
                    + CommandExceptionMap.get(qr).getFaultString() +"\n";
            }
            throw new UserActionException(errMsg);
        }
    }

    /*public boolean isDestroyed() {
	return destroyed;
	}*/
    /**----------------------------------------------------------------------
     * get all XDAQ executives and kill them
     */
    protected void destroyXDAQ()
        throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_FMname + "] GEMFunctionManager::destroyXDAQ(): ";

        logger.info(msgPrefix + "destroyXDAQ called");
	// QualifiedGroup qg = getQualifiedGroup();

        /* ** THIS CAUSES FAILURE, POSSIBLY DUE TO FWK BUG WITH LIGHT CONFIGS **
	// see if there is an exec with a supervisor and kill it first
	URI supervExecURI = null;
	if (c_gemSupervisors != null) {
            if (!c_gemSupervisors.isEmpty()) {
                logger.info(msgPrefix + "killing GEMSupervisor executives (" + c_gemSupervisors.getApplications().size() + ")");
                for (QualifiedResource qr : c_gemSupervisors.getApplications()) {
                    // seems to not require the looping
                    // Resource supervResource = c_gemSupervisors.getApplications().get(0).getResource();
                    logger.info(msgPrefix + "killing executive for supervisor process " + qr.getName());
                    Resource supervResource = qr.getResource();
                    logger.info(msgPrefix + "got supervisor resource " + qr.getName());
                    try {
                        XdaqExecutiveResource qrSupervParentExec =
                            ((XdaqApplicationResource)supervResource).getXdaqExecutiveResourceParent();
                        logger.info(msgPrefix + "got supervisor executive " + qrSupervParentExec.getApplicationClassName());
                        supervExecURI = qrSupervParentExec.getURI();
                        QualifiedResource qrExec = m_gemQG.seekQualifiedResourceOfURI(supervExecURI);
                        XdaqExecutive     ex     = (XdaqExecutive) qrExec;
                        logger.info(msgPrefix + "killing supervisor executive with URI " + supervExecURI.toString()
                                    + ", executive initialized: " + ex.isInitialized());
                        try {
                            logger.info(msgPrefix + "killing supervisor executive " + ex.getName());
                            // ex.destroy();
                            ex.killMe();
                        } catch (Exception e) {
                            String msg = "Exception when destroying supervisor executive named:" + ex.getName()
                                + " with URI " + ex.getURI().toString();
                            logger.error(msgPrefix + msg, e);
                            goToError(msg, e);
                            throw (UserActionException) e;
                        }
                    } catch (Exception e) {
                        String msg = "Exception when gettting supervisor executive";
                        logger.error(msgPrefix + msg, e);
                        goToError(msg, e);
                        throw (UserActionException) e;
                    }
                }
                logger.info(msgPrefix + "done killing supervisor executives");
            } else {
                logger.warn(msgPrefix + "unable to find GEMSupervisor executives");
            }
        } else {
            logger.warn(msgPrefix + "unable to find GEMSupervisor container");
        }
        */

	// find all XDAQ executives and kill them
	if (m_gemQG != null) {
            List<QualifiedResource> qrList = m_gemQG.seekQualifiedResourcesOfType(new XdaqExecutive());
            logger.info(msgPrefix + "killing all other executives(" + qrList.size() + ") in the QualifiedGroup");
            for (QualifiedResource qr : qrList) {
                logger.info(msgPrefix + "killing executive " + qr.getName());
                XdaqExecutive exec = (XdaqExecutive)qr;
                // logger.info(msgPrefix + "supervisor URI:" + supervExecURI.toString()
                logger.info(msgPrefix
                            + ", executive URI" + exec.getURI().toString()
                            + ", executive initialized: " + exec.isInitialized());
                // if (!exec.getURI().equals(supervExecURI)) {
                try {
                    logger.info(msgPrefix + "killing executive " + exec.getName());
                    exec.destroy();
                    // exec.killMe();
                } catch ( Exception e) {
                    String msg = "Exception when destroying executive named:" + exec.getName()
                        + " with URI " + exec.getURI().toString();
                    logger.error(msgPrefix + msg, e);
                    goToError(msg,e);
                    throw (UserActionException) e;
                }
                // }
            }

            logger.info(msgPrefix + "done killing executives");
        } else {
            logger.warn(msgPrefix + "unable to find the QualifiedGroup");
        }

	// reset the qualified group so that the next time an init is sent all resources will be initialized again
	// QualifiedGroup qg = getQualifiedGroup();
        logger.info(msgPrefix + "resetting the QualifiedGroup");
	if (m_gemQG != null) {
            m_gemQG.reset();
        }

        logger.info(msgPrefix + "done!");
    }


    private void submitTaskList(TaskSequence taskList)
        throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_FMname + "] GEMFunctionManager::submitTaskList(TaskSequence): ";

        // Make sure that task list belongs to active state we are in
        if (!taskList.getState().equals(this.getState())) {
            String msg = "taskList does not belong to this state \n "
                + "Function Manager state = " + this.getState() + "\n"
                + "taskList is for state = " + taskList.getState();
            logger.error(msg);
            throw new UserActionException(msg);
        }

        try {
            logger.info(msgPrefix + " before taskList.completion(): " + taskList.completion());
            taskList.startExecution();
            logger.info(msgPrefix + " after taskList.completion(): " + taskList.completion());
        } catch (EventHandlerException e) {
            String msg = e.getMessage();
            this.getParameterSet().get(GEMParameters.ERROR_MSG)
                .setValue(new StringT(msg));
            throw new UserActionException("Could not start execution of " + taskList.getDescription(), e);
        }

        // do a while loop to cover synchronous tasks which finish immediately
        Task activeTask = null;

        while ( activeTask == null || activeTask.isCompleted()) {
            if (activeTask != null)
                logger.info(msgPrefix + " activeTask: " + activeTask + " completed.");

            if (taskList.isEmpty()) {
                logger.warn(msgPrefix + " taskList is empty, tasks may have completed");
                this.getParameterSet().get(GEMParameters.ACTION_MSG)
                    .setValue(new StringT("Tasks completed."));
                this.fireEvent(taskList.getCompletionEvent());
                activeTask       = null;
                taskList = null;
                break;
            } else {
                activeTask = (Task)taskList.removeFirst();
                logger.info(msgPrefix + " Start new task: " + activeTask.getDescription());
                this.getParameterSet().get(GEMParameters.ACTION_MSG)
                    .setValue( new StringT("Executing: " + activeTask.getDescription()));
                try {
                    activeTask.startExecution();
                } catch (EventHandlerException e) {
                    String msg = e.getMessage();
                    this.getParameterSet().get(GEMParameters.ERROR_MSG)
                        .setValue(new StringT(msg));
                    throw new UserActionException("Could not start execution of "+ activeTask.getDescription(), e);
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see rcms.statemachine.user.IUserStateMachine#getUpdatedState()
     */
    public State getUpdatedState()
    {
        String msgPrefix = "[GEM FM::" + m_FMname + "] GEMFunctionManager::getUpdatedState(): ";

        // This method is called by the framework when a Function Manager is
        // created (after createAction).
        // It can be used for updating the state of the Function Manager
        // according to the resources' state.
        //
        // In this example it is used also to retrieve the updated State of all
        // resources
        // and recalculate the State Machine state.

        if (m_gemQG == null || !m_gemQG.isInitialized()) {
            // if the QualifiedGroup is not initialized just return the FSM
            // state.
            return this.getState();
        }

        // inquiry the state of the resources
        // m_gemQG.getStates();
        // recalculate the state
        if (m_svCalc != null) {
            m_calcState = m_svCalc.getState();
        }
        return m_calcState;
    }

    /**
     * Example of calculation of State derived from the State of the controlled
     * resources.
     */
    public void defineConditionState()
    {
        String msgPrefix = "[GEM FM::" + m_FMname + "] GEMFunctionManager::defineConditionState(): ";

        // The getAll methods must be called only when the m_gemQG is
        // initialized.
        logger.debug(msgPrefix + "defineConditionState");

        // Conditions for State OFF
        StateVector initialConds = new StateVector();
        initialConds.registerConditionState(c_tcdsControllers, GEMStates.HALTED);
        initialConds.registerConditionState(c_uFEDKIT,         GEMStates.HALTED);
        initialConds.registerConditionState(c_gemSupervisors,  GEMStates.INITIAL);
        initialConds.registerConditionState(c_FMs,             GEMStates.INITIAL);
        initialConds.registerConditionState(c_uFEDKIT,         GEMStates.HALTED);
        initialConds.setResultState(GEMStates.INITIAL);

        // Conditions for State HALTED
        StateVector haltedConds = new StateVector();
        haltedConds.registerConditionState(c_tcdsControllers, GEMStates.HALTED);
        haltedConds.registerConditionState(c_uFEDKIT,         GEMStates.HALTED);
        haltedConds.registerConditionState(c_gemSupervisors,  GEMStates.HALTED);
        haltedConds.registerConditionState(c_FMs,             GEMStates.HALTED);
        haltedConds.setResultState(GEMStates.HALTED);

        // Conditions for State CONFIGURED
        StateVector configuredConds = new StateVector();
        configuredConds.registerConditionState(c_tcdsControllers, GEMStates.CONFIGURED);
        configuredConds.registerConditionState(c_uFEDKIT,         GEMStates.CONFIGURED);
        configuredConds.registerConditionState(c_gemSupervisors,  GEMStates.CONFIGURED);
        configuredConds.registerConditionState(c_FMs,             GEMStates.CONFIGURED);
        configuredConds.setResultState(GEMStates.CONFIGURED);

        // Conditions for State RUNNING/ENABLED
        StateVector runningConds = new StateVector();
        runningConds.registerConditionState(c_tcdsControllers, GEMStates.RUNNING);
        runningConds.registerConditionState(c_uFEDKIT,         GEMStates.RUNNING);
        runningConds.registerConditionState(c_gemSupervisors,  GEMStates.RUNNING);
        runningConds.registerConditionState(c_FMs,             GEMStates.RUNNING);
        runningConds.setResultState(GEMStates.RUNNING);

        // // Conditions for State RUNNINGDEGRADED
        // StateVector runningdegradedConds = new StateVector();
        // runningdegradedConds.registerConditionState(c_tcdsControllers, GEMStates.RUNNINGDEGRADED);
        // runningdegradedConds.registerConditionState(c_uFEDKIT,         GEMStates.RUNNINGDEGRADED);
        // runningdegradedConds.registerConditionState(c_gemSupervisors,  GEMStates.RUNNINGDEGRADED);
        // runningdegradedConds.registerConditionState(c_FMs,             GEMStates.RUNNINGDEGRADED);
        // runningdegradedConds.setResultState(GEMStates.RUNNINGDEGRADED);

        // // Conditions for State RUNNINGSOFTERRORDETECTED
        // StateVector runningsofterrordetectedConds = new StateVector();
        // runningsofterrordetectedConds.registerConditionState(c_tcdsControllers, GEMStates.RUNNINGSOFTERRORDETECTED);
        // runningsofterrordetectedConds.registerConditionState(c_uFEDKIT,         GEMStates.RUNNINGSOFTERRORDETECTED);
        // runningsofterrordetectedConds.registerConditionState(c_gemSupervisors,  GEMStates.RUNNINGSOFTERRORDETECTED);
        // runningsofterrordetectedConds.registerConditionState(c_FMs,             GEMStates.RUNNINGSOFTERRORDETECTED);
        // runningsofterrordetectedConds.setResultState(GEMStates.RUNNINGSOFTERRORDETECTED);

        // // Conditions for State PAUSED
        // StateVector pausedConds = new StateVector();
        // pausedConds.registerConditionState(c_tcdsControllers, GEMStates.PAUSED);
        // pausedConds.registerConditionState(c_uFEDKIT,         GEMStates.PAUSED);
        // pausedConds.registerConditionState(c_gemSupervisors,  GEMStates.PAUSED);
        // pausedConds.registerConditionState(c_FMs,             GEMStates.PAUSED);
        // pausedConds.setResultState(GEMStates.PAUSED);

        // Conditions for State ERROR
        StateVector errorConds = new StateVector();
        errorConds.registerConditionState(c_tcdsControllers, GEMStates.ERROR);
        errorConds.registerConditionState(c_uFEDKIT,         GEMStates.ERROR);
        errorConds.registerConditionState(c_gemSupervisors,  GEMStates.ERROR);
        errorConds.registerConditionState(c_FMs,             GEMStates.ERROR);
        errorConds.setResultState(GEMStates.ERROR);

        // Add the conditions and all the resources belonging to this group
        // to the object that calculates the State.
        Set resourceGroup = m_gemQG.getQualifiedResourceGroup();
        m_svCalc = new StateVectorCalculation(resourceGroup);
        m_svCalc.add(initialConds);
        m_svCalc.add(haltedConds);
        m_svCalc.add(configuredConds);
        m_svCalc.add(runningConds);
        // m_svCalc.add(runningdegradedConds);
        // m_svCalc.add(runningsofterrordetectedConds);
        // m_svCalc.add(pausedConds);
        m_svCalc.add(errorConds);

        logger.debug(msgPrefix + "Condition States defined");
    }
}
