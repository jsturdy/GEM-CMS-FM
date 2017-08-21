package rcms.fm.app.gemfm;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.TimeZone;

import java.net.URI;
import java.net.URL;
import java.net.MalformedURLException;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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

import rcms.resourceservice.db.resource.Resource;
import rcms.resourceservice.db.resource.xdaq.XdaqApplicationResource;
import rcms.resourceservice.db.resource.xdaq.XdaqExecutiveResource;

import net.hep.cms.xdaqctl.WSESubscription; // what is this for?

import rcms.statemachine.definition.Input;
import rcms.statemachine.definition.State;
import rcms.statemachine.definition.StateMachineDefinitionException;

import rcms.resourceservice.db.resource.fm.FunctionManagerResource;

import rcms.util.logger.RCMSLogger;

import rcms.errorFormat.CMS.CMSError;

import rcms.util.logsession.LogSessionException;
import rcms.util.logsession.LogSessionConnector;

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
    public LogSessionConnector logSessionConnector;  // Connector for logsession DB

    /**
     * define some containers
     */
    public XdaqApplicationContainer containerXdaqApplication        = null;  ///<
    public XdaqApplicationContainer containerXdaqServiceApplication = null;  ///<

    /**
     * define specific application containers
     */
    public XdaqApplicationContainer containerGEMSupervisor   = null;  ///<
    public XdaqApplicationContainer containerAMC13Manager    = null;  ///<
    public XdaqApplicationContainer containerICIController   = null;  ///<
    public XdaqApplicationContainer containerPIController    = null;  ///<
    public XdaqApplicationContainer containerLPMController   = null;  ///<
    public XdaqApplicationContainer containerTCDSControllers = null;  ///<
    public XdaqApplicationContainer containerTTCciControl    = null;  ///<
    public XdaqApplicationContainer containerFEDKIT          = null;  ///<
    public XdaqApplicationContainer containerBU              = null;  ///<
    public XdaqApplicationContainer containerRU              = null;  ///<
    public XdaqApplicationContainer containerEVM             = null;  ///<
    public XdaqApplicationContainer containerFerol           = null;  ///<
    public XdaqApplicationContainer containerFEDStreamer     = null;  ///<

    /**
     * <code>containerXdaqExecutive</code>: container of XdaqExecutive in the
     * running Group.
     */
    public XdaqApplicationContainer containerXdaqExecutive = null;

    /**
     * <code>containerFunctionManager</code>: container of FunctionManagers
     * in the running Group.
     */
    public QualifiedResourceContainer containerFunctionManager = null;

    /**
     * <code>containerJobControl</code>: container of JobControl in the
     * running Group.
     */
    public QualifiedResourceContainer containerJobControl = null;

    /**
     * <code>svCalc</code>: Composite state calculator
     */
    public StateVectorCalculation svCalc = null;

    /**
     * <code>calcState</code>: Calculated State.
     */
    public State calcState = null;

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
    public String  RunType = "";
    public Integer RunNumber = 0;
    //public Integer CachedRunNumber = 0;

    // GEM RunInfo namespace, the FM name will be added in the createAction() method
    public String GEM_NS = "CMS.";

    // string containing details on the setup from where this FM was started (copied from HCAL)
    public String rcmsStateListenerURL = "";
    public String RunSetupDetails  = "empty";
    public String FMfullpath       = "empty";
    public String FMname           = "empty";
    public String FMurl            = "empty";
    public String FMuri            = "empty";
    public String FMrole           = "empty";
    public String FMpartition      = "empty";
    public String utcFMtimeofstart = "empty";
    public Date FMtimeofstart;

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

        String msg = "[GEM FM::" + FMname + "] createAction called.";
        System.out.println(msg);
        logger.debug(msg);

        // Retrieve the configuration for this Function Manager from the Group
        FunctionManagerResource fmConf = ((FunctionManagerResource) qualifiedGroup.getGroup().getThisResource());

        FMfullpath    = fmConf.getDirectory().getFullPath().toString();
        FMname        = fmConf.getName();
        FMurl         = fmConf.getSourceURL().toString();
        FMuri         = fmConf.getURI().toString();
        FMrole        = fmConf.getRole();
        FMtimeofstart = new Date();
        DateFormat dateFormatter = new SimpleDateFormat("M/d/yy hh:mm:ss a z");
        dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));;
        utcFMtimeofstart = dateFormatter.format(FMtimeofstart);

        // set statelistener URL
        try {
            URL fmURL = new URL(FMurl);
            String rcmsStateListenerHost = fmURL.getHost();
            int rcmsStateListenerPort = fmURL.getPort()+1;
            String rcmsStateListenerProtocol = fmURL.getProtocol();
            rcmsStateListenerURL = rcmsStateListenerProtocol+"://"+rcmsStateListenerHost+":"+rcmsStateListenerPort+"/rcms";
        } catch (MalformedURLException e) {
            String errMsg = "[GEM FM::" + FMname + "] Error! MalformedURLException in createAction" + e.getMessage();
            logger.error(errMsg,e);
            sendCMSError(errMsg);
            getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
            getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(errMsg)));
            // if (theEventHandler.TestMode.equals("off")) { firePriorityEvent(GEMInputs.SETERROR); ErrorState = true; return;}
        }

        // get log session connector
        logger.info("[GEM FM::" + FMname + "] Get log session connector started");
        logSessionConnector = getLogSessionConnector();
        logger.info("[GEM FM::" + FMname + "] Get log session connector finished");

        // get session ID // NEEDS TO BE REMOVED FOR GLOBAL OPERATIONS
        logger.info("[GEM FM::" + FMname + "] Get session ID started");
        getSessionId();
        logger.info("[GEM FM::" + FMname + "] Get session ID finished");

        logger.debug("[GEM FM::" + FMname + "] createAction executed.");
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
        //qualifiedGroup.destroy();

        System.out.println("destroyAction called");
        logger.debug("destroyAction called");

        // try to close any open session ID only if we are in local run mode i.e. not CDAQ and not miniDAQ runs and if it's a LV1FM
        //if (RunType.equals("local") && !containerFMChildren.isEmpty()) { closeSessionId(); }
        closeSessionId(); //NEEDS TO BE CORRECTED TO ONLY BE CALLED IN LOCAL RUNS

        try {
            // retrieve the Function Managers and kill themDestroy all XDAQ applications
            destroyXDAQ();
        } catch (UserActionException e){
            String msg = "[GEM FM::" + FMname + " ] destroyAction: Got an exception during destroyXDAQ()";
            logger.error(msg + ": " + e);
            goToError(msg,e);
            throw e;
        }

        // make sure we send HALT to all TCDS applications
        if (containerTCDSControllers != null){
            if (!containerTCDSControllers.isEmpty()) {
                try {
                    logger.info("[GEM FM::" + FMname + "] Trying to halt TCDS on destroy.");
                    haltTCDSControllers();
                } catch (UserActionException e) {
                    String msg = "[GEM FM::" + FMname + "] destroyAction: got an exception while halting TCDS";
                    logger.error(msg + ": " + e);
                    goToError(msg,e);
                }
            }
        }
        // } catch (UserActionException e){
        //     String errMsg = "[GEM FM::" + FMname + " ] Got an exception during destroyAction():";
        //     goToError(errMsg,e);
        //     throw e;
        // }

        String msg = "[GEM FM::" + FMname + "] destroyAction: destroying the Qualified Group";
        logger.error(msg);
        this.getQualifiedGroup().destroy();

        System.out.println("destroyAction executed");
        logger.debug("destroyAction executed");
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

        //
        // Set first of all the State Machine Definition
        //
        setStateMachineDefinition(new GEMStateMachineDefinition());

        //
        // Add event handler
        //
        addEventHandler(new GEMEventHandler());

        //
        // Add error handler
        //
        addEventHandler(new GEMErrorHandler());

    }

    // get a session Id
    @SuppressWarnings("unchecked") // SHOULD REALLY MAKE SURE THAT THIS IS NECESSARY AND NOT JUST DUE TO BAD JAVA
        protected void getSessionId()
    {
        String user        = getQualifiedGroup().getGroup().getDirectory().getUser();
        String description = getQualifiedGroup().getGroup().getDirectory().getFullPath();
        int sessionId      = 0;

        logger.debug("[GEM base] Log session connector: " + logSessionConnector );

        if (logSessionConnector != null) {
            try {
                sessionId = logSessionConnector.createSession( user, description );
                logger.info("[GEM base] New session Id obtained =" + sessionId );
            } catch (LogSessionException e1) {
                logger.warn("[GEM base] Could not get session ID, using default = " + sessionId + ". Exception: ",e1);
            }
        } else {
            logger.warn("[GEM base] logSessionConnector = " + logSessionConnector + ", using default = " + sessionId + ".");
        }

        // put the session ID into parameter set
        logger.info("[GEM base] setting SID to " + sessionId);
        getParameterSet().get(GEMParameters.SID).setValue(new IntegerT(sessionId));
        logger.info("[GEM base] parameter values of SID is "
                    + getParameterSet().get(GEMParameters.SID).getValue());
    }

    // close session Id. This routine is called always when functionmanager gets destroyed.
    protected void closeSessionId()
    {
        if (logSessionConnector != null) {
            int sessionId = 0;
            try {
                sessionId = ((IntegerT)getParameterSet().get(GEMParameters.SID).getValue()).getInteger();
            } catch (Exception e) {
                logger.warn("[GEM FM::" + FMname + "] Could not get sessionId for closing session.\n"
                            + "Not closing session.\n"
                            + "(This is OK if no sessionId was requested from within GEM land, i.e. global runs)."
                            + "Exception: ", e);
            }
            try {
                logger.debug("[GEM FM::" + FMname + "] Trying to close log sessionId = " + sessionId );
                logSessionConnector.closeSession(sessionId);
                logger.debug("[GEM FM::" + FMname + "] ... closed log sessionId = " + sessionId );
            } catch (LogSessionException e1) {
                logger.warn("[GEM FM::" + FMname + "] Could not close sessionId, but sessionId was requested and used.\n"
                            + "This is OK only for global runs.\n"
                            + "Exception: ", e1);
            }
        } else {
            logger.warn("[GEM base] logSessionConnector null");
        }
    }

    public boolean isDegraded()
    {
        // FM may check whether it is currently degraded if such functionality exists
        return degraded;
    }

    public boolean hasSoftError()
    {
        // FM may check whether the system has a soft error if such functionality exists
        return softErrorDetected;
    }

    // only needed if FM cannot check for degradation
    public void setDegraded(boolean degraded)
    {
        this.degraded = degraded;
    }

    // only needed if FM cannot check for softError
    public void setSoftErrorDetected(boolean softErrorDetected)
    {
        this.softErrorDetected = softErrorDetected;
    }

    @SuppressWarnings("unchecked")
        protected void sendCMSError(String errMessage)
    {
        // create a new error notification msg
        CMSError error = getErrorFactory().getCMSError();
        error.setDateTime(new Date().toString());
        error.setMessage(errMessage);

        // update error msg parameter for GUI
        getParameterSet().get("ERROR_MSG").setValue(new StringT(errMessage));

        // send error
        try {
            getParentErrorNotifier().sendError(error);
        } catch (Exception e) {
            logger.warn("[GEM FM::" + FMname + "] " + getClass().toString() + ": Failed to send error message " + errMessage);
        }
    }


    /**----------------------------------------------------------------------
     * go to the error state, setting messages and so forth, with exception
     */
    public void goToError(String errMessage, Exception e)
    {
        errMessage += " Message from the caught exception is: "+e.getMessage();
        goToError(errMessage);
    }

    /**----------------------------------------------------------------------
     * go to the error state, setting messages and so forth, without exception
     */
    public void goToError(String errMessage)
    {
        logger.error(errMessage);
        sendCMSError(errMessage);
        getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
        getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(errMessage)));
        getParameterSet().put(new FunctionManagerParameter<StringT>("SUPERVISOR_ERROR",new StringT(errMessage)));
        Input errInput = new Input(GEMInputs.SETERROR);
        errInput.setReason(errMessage);
        // if (theEventHandler.TestMode.equals("off")) { firePriorityEvent(errInput); ErrorState = true; }
    }

    /**----------------------------------------------------------------------
     * halt the TCDS controllers
     */
    public void haltTCDSControllers()
        throws UserActionException
    {
        // ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
        // int sessionId = ((IntegerT)getParameterSet().get(GEMParameters.SID).getValue()).getInteger();
        // pSet.put(new FunctionManagerParameter<IntegerT>("SID", new IntegerT(sessionId)));

        try {
            if (!containerLPMController.isEmpty()) {
                logger.info("[GEM FM::" + FMname + "]  Sending halt to LPM ");
                containerLPMController.execute(GEMInputs.HALT);
                // if LPM is not a service app, need to provide rcmsURL
                //lpmApp.execute(GEMInputs.HALT,"test",rcmsStateListenerURL);
            }
            if (!containerICIController.isEmpty()) {
                logger.info("[GEM FM::" + FMname + "]  Sending halt to iCI ");
                containerICIController.execute(GEMInputs.HALT);
            }
            if (!containerPIController.isEmpty()) {
                logger.info("[GEM FM::" + FMname + "]  Sending halt to PI ");
                containerPIController.execute(GEMInputs.HALT);
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
    public void configureTCDSControllers()
        throws UserActionException
    {
        try {
            TaskSequence configureTaskSeq = new TaskSequence(GEMStates.CONFIGURING,GEMInputs.SETCONFIGURE);

            if (!containerLPMController.isEmpty()) {
                logger.info("[GEM FM::" + FMname + "]  Sending configure to LPM ");
                // // add LPM configuration string to CommandParameter
                // ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("LPM_HW_CFG", new StringT("N/A")));
                // // prepare command plus the parameters to send
                // Input configureInput= new Input(GEMInputs.CONFIGURE.toString());
                // configureInput.setParameters( pSet );
                // SimpleTask lpmConfigureTask = new SimpleTask(containerLPMController,configureInput,
                //                                              GEMStates.CONFIGURING,GEMStates.CONFIGURED,
                //                                              "Configuring LPMControllers");
                // configureTaskSeq.addLast(lpmConfigureTask);
                containerLPMController.execute(GEMInputs.CONFIGURE);
                // if LPM is not a service app, need to provide rcmsURL
                //lpmApp.execute(GEMInputs.CONFIGURE,"test",rcmsStateListenerURL);
            }
            if (!containerPIController.isEmpty()) {
                logger.info("[GEM FM::" + FMname + "]  Sending configure to PI ");
                // // add PI configuration string to CommandParameter
                // ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("PI_HW_CFG", new StringT("N/A")));
                // // prepare command plus the parameters to send
                // Input configureInput= new Input(GEMInputs.CONFIGURE.toString());
                // configureInput.setParameters( pSet );
                // SimpleTask piConfigureTask = new SimpleTask(containerPIController,configureInput,
                //                                             GEMStates.CONFIGURING,GEMStates.CONFIGURED,
                //                                             "Configuring PIControllers");

                // configureTaskSeq.addLast(piConfigureTask);
                containerPIController.execute(GEMInputs.CONFIGURE);
            }
            if (!containerICIController.isEmpty()) {
                logger.info("[GEM FM::" + FMname + "]  Sending configure to iCI ");
                // // add ICI configuration string to CommandParameter
                // ParameterSet<CommandParameter> pSet = new ParameterSet<CommandParameter>();
                // pSet.put(new CommandParameter<StringT>("ICI_HW_CFG", new StringT("N/A")));
                // // prepare command plus the parameters to send
                // Input configureInput= new Input(GEMInputs.CONFIGURE.toString());
                // configureInput.setParameters( pSet );
                // SimpleTask iciConfigureTask = new SimpleTask(containerICIController,configureInput,
                //                                              GEMStates.CONFIGURING,GEMStates.CONFIGURED,
                //                                              "Configuring ICIControllers");

                // configureTaskSeq.addLast(iciConfigureTask);
                containerICIController.execute(GEMInputs.CONFIGURE);
            }
            // this.theStateNotificationHandler.executeTaskSequence(configureTaskSeq);
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
        try {
            if (!containerLPMController.isEmpty()) {
                logger.info("[GEM FM::" + FMname + "]  Sending enable to LPM ");
                containerLPMController.execute(GEMInputs.ENABLE);
                // if LPM is not a service app, need to provide rcmsURL
                //lpmApp.execute(GEMInputs.ENABLE,"test",rcmsStateListenerURL);
            }
            if (!containerPIController.isEmpty()) {
                logger.info("[GEM FM::" + FMname + "]  Sending enable to PI ");
                containerPIController.execute(GEMInputs.ENABLE);
            }
            if (!containerICIController.isEmpty()) {
                logger.info("[GEM FM::" + FMname + "]  Sending enable to iCI ");
                containerICIController.execute(GEMInputs.ENABLE);
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
        try {
            if (!containerLPMController.isEmpty()) {
                logger.info("[GEM FM::" + FMname + "]  Sending stop to LPM ");
                containerLPMController.execute(GEMInputs.STOP);
                // if LPM is not a service app, need to provide rcmsURL
                //lpmApp.execute(GEMInputs.STOP,"test",rcmsStateListenerURL);
            }
            if (!containerICIController.isEmpty()) {
                logger.info("[GEM FM::" + FMname + "]  Sending stop to iCI ");
                containerICIController.execute(GEMInputs.STOP);
            }
            if (!containerPIController.isEmpty()) {
                logger.info("[GEM FM::" + FMname + "]  Sending stop to PI ");
                containerPIController.execute(GEMInputs.STOP);
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
        try {
            if (!containerLPMController.isEmpty()) {
                logger.info("[GEM FM::" + FMname + "]  Sending pause to LPM ");
                containerLPMController.execute(GEMInputs.PAUSE);
                // if LPM is not a service app, need to provide rcmsURL
                //lpmApp.execute(GEMInputs.PAUSE,"test",rcmsStateListenerURL);
            }
            if (!containerICIController.isEmpty()) {
                logger.info("[GEM FM::" + FMname + "]  Sending pause to iCI ");
                containerICIController.execute(GEMInputs.PAUSE);
            }
            if (!containerPIController.isEmpty()) {
                logger.info("[GEM FM::" + FMname + "]  Sending pause to PI ");
                containerPIController.execute(GEMInputs.PAUSE);
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

    /**----------------------------------------------------------------------
     * get all XDAQ executives and kill them
     */
    protected void destroyXDAQ()
        throws UserActionException
    {
        logger.info("[GEM FM::" + FMname + "] destroyXDAQ called");
	QualifiedGroup qg = getQualifiedGroup();

	// see if there is an exec with a supervisor and kill it first
	URI supervExecURI = null;
	if (containerGEMSupervisor != null) {
            if (!containerGEMSupervisor.isEmpty()) {
                logger.info("[GEM FM::" + FMname + "] destroyXDAQ: killing GEMSupervisor executives ("
                            + containerGEMSupervisor.getApplications().size() + ")");
                for (QualifiedResource qr : containerGEMSupervisor.getApplications()) {
                    // Resource supervResource = containerGEMSupervisor.getApplications().get(0).getResource();
                    logger.info("[GEM FM::" + FMname + "] destroyXDAQ: killing executive for supervisor process "
                                + qr.getName());
                    Resource supervResource = qr.getResource();
                    logger.info("[GEM FM::" + FMname + "] destroyXDAQ: got supervisor resource " + qr.getName());
                    XdaqExecutiveResource qrSupervParentExec =
                        ((XdaqApplicationResource)supervResource).getXdaqExecutiveResourceParent();
                    logger.info("[GEM FM::" + FMname + "] destroyXDAQ: got supervisor executive "
                                + qrSupervParentExec.getApplicationClassName());
                    supervExecURI = qrSupervParentExec.getURI();
                    QualifiedResource qrExec = qualifiedGroup.seekQualifiedResourceOfURI(supervExecURI);
                    XdaqExecutive     ex     = (XdaqExecutive) qrExec;
                    logger.info("[GEM FM::" + FMname + "] destroyXDAQ: killing supervisor executive with URI "
                                + supervExecURI.toString()
                                + ", executive initialized: " + ex.isInitialized());
                    try {
                        logger.info("[GEM FM::" + FMname + "] destroyXDAQ: killing supervisor executive " + ex.getName());
                        // ex.destroy();
                        ex.killMe();
                    } catch ( Exception e) {
                        String msg = "[GEM "+FMname+"] destroyXDAQ: Exception when destroying supervisor executive named:"
                            + ex.getName()
                            + " with URI " + ex.getURI().toString();
                        logger.error(msg + ": " + e);
                        goToError(msg,e);
                        throw (UserActionException) e;
                    }
                }
                logger.info("[GEM FM::" + FMname + "] destroyXDAQ: done killing supervisor executives");
            } else {
                logger.warn("[GEM FM::" + FMname + "] destroyXDAQ: unable to find GEMSupervisor executives");
            }
        } else {
            logger.warn("[GEM FM::" + FMname + "] destroyXDAQ: unable to find GEMSupervisor container");
        }

	// find all XDAQ executives and kill them
	if (qualifiedGroup != null) {
            // if (!qualifiedGroup.isEmpty()) {
            List<QualifiedResource> qrList = qg.seekQualifiedResourcesOfType(new XdaqExecutive());
            logger.info("[GEM FM::" + FMname + "] destroyXDAQ: killing all other executives("
                        + qrList.size() + ") in the QualifiedGroup");
            for (QualifiedResource qr : qrList) {
                logger.info("[GEM FM::" + FMname + "] destroyXDAQ: killing executive " + qr.getName());
                XdaqExecutive exec = (XdaqExecutive)qr;
                logger.info("[GEM FM::" + FMname + "] destroyXDAQ: supervisor URI:" + supervExecURI.toString()
                            + ", executive URI" + exec.getURI().toString()
                            + ", executive initialized: " + exec.isInitialized());
                if (!exec.getURI().equals(supervExecURI))
                    try {
                        logger.info("[GEM FM::" + FMname + "] destroyXDAQ: killing executive " + exec.getName());
                        // exec.destroy();
                        exec.killMe();
                    } catch ( Exception e) {
                        String msg = "[GEM "+FMname+"] destroyXDAQ: Exception when destroying executive named:" + exec.getName()
                            + " with URI " + exec.getURI().toString();
                        logger.error(msg + ": " + e);
                        goToError(msg,e);
                        throw (UserActionException) e;
                    }
            }

            // List listExecutive = qualifiedGroup.seekQualifiedResourcesOfType(new XdaqExecutive());
            // Iterator it = listExecutive.iterator();
            // while (it.hasNext()) {
            //     XdaqExecutive ex = (XdaqExecutive) it.next();
            //     if (!ex.getURI().equals(supervExecURI)) {
            //         ex.destroy();
            //     }
            // }
            logger.info("[GEM FM::" + FMname + "] destroyXDAQ: done killing executives");
            // } else {
            //     logger.warn("[GEM FM::" + FMname + "] destroyXDAQ: unable to find executives in the QualifiedGroup");
            // }
        } else {
            logger.warn("[GEM FM::" + FMname + "] destroyXDAQ: unable to find the QualifiedGroup");
        }

	// reset the qualified group so that the next time an init is sent all resources will be initialized again
	//QualifiedGroup qg = getQualifiedGroup();
        logger.info("[GEM FM::" + FMname + "] destroyXDAQ: resetting the QualifiedGroup");
	if (qg != null) { qg.reset(); }

        logger.info("[GEM FM::" + FMname + "] destroyXDAQ: done!");
    }
}
