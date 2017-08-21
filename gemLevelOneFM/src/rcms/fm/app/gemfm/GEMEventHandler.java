package rcms.fm.app.gemfm;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Calendar;
import java.util.Random;

import java.io.StringWriter;
import java.io.PrintWriter;

import rcms.errorFormat.CMS.CMSError;
import rcms.fm.fw.StateEnteredEvent;
import rcms.fm.fw.parameter.CommandParameter;
import rcms.fm.fw.parameter.FunctionManagerParameter;
import rcms.fm.fw.parameter.ParameterSet;
import rcms.fm.fw.parameter.type.IntegerT;
import rcms.fm.fw.parameter.type.LongT;
import rcms.fm.fw.parameter.type.StringT;
import rcms.fm.fw.parameter.type.DoubleT;
import rcms.fm.fw.parameter.type.VectorT;
import rcms.fm.fw.user.UserActionException;
import rcms.fm.fw.user.UserStateNotificationHandler;
import rcms.fm.resource.QualifiedGroup;
import rcms.fm.resource.QualifiedResource;
import rcms.fm.resource.QualifiedResourceContainer;
import rcms.fm.resource.QualifiedResourceContainerException;
import rcms.fm.resource.qualifiedresource.XdaqApplication;
import rcms.fm.resource.qualifiedresource.XdaqServiceApplication;
import rcms.fm.resource.qualifiedresource.XdaqApplicationContainer;
import rcms.fm.resource.qualifiedresource.XdaqExecutive;
import rcms.fm.resource.qualifiedresource.XdaqExecutiveConfiguration;
import rcms.xdaqctl.XDAQParameter;
import rcms.xdaqctl.XDAQMessage;
import rcms.fm.resource.qualifiedresource.JobControl;
import rcms.fm.resource.qualifiedresource.FunctionManager;
import rcms.resourceservice.db.resource.fm.FunctionManagerResource;
import rcms.stateFormat.StateNotification;
import rcms.util.logger.RCMSLogger;

import net.hep.cms.xdaqctl.XDAQException;
import net.hep.cms.xdaqctl.XDAQTimeoutException;
import net.hep.cms.xdaqctl.XDAQMessageException;

import rcms.util.logsession.LogSessionException;
import rcms.util.logsession.LogSessionConnector;

import rcms.utilities.runinfo.RunNumberData;
import rcms.utilities.runinfo.RunSequenceNumber;
import rcms.utilities.runinfo.RunInfo;
import rcms.utilities.runinfo.RunInfoException;
import rcms.utilities.runinfo.RunInfoConnectorIF;

/**
 *
 * Main Event Handler class for GEM Level 1 Function Manager.
 *
 * @author Andrea Petrucci, Alexander Oh, Michele Gulmini
 * @maintainer Jose Ruiz, Jared Sturdy
 *
 */

public class GEMEventHandler extends UserStateNotificationHandler {

    GEMFunctionManager functionManager = null;

    /**
     * <code>RCMSLogger</code>: RCMS log4j logger.
     */
    static RCMSLogger logger = new RCMSLogger(GEMEventHandler.class);

    private QualifiedGroup qualifiedGroup = null;

    public Integer Sid              =  0;           // Session ID for database connections
    public String RunSequenceName   =  "GEM test"; // Run sequence name, for attaining a run sequence number
    public Integer RunSeqNumber     =  0;

    public GEMEventHandler()
        throws rcms.fm.fw.EventHandlerException
    {
        // this handler inherits UserStateNotificationHandler
        // so it is already registered for StateNotification events

        // Let's register also the StateEnteredEvent triggered when the FSM enters in a new state.
        subscribeForEvents(StateEnteredEvent.class);

        addAction(GEMStates.INITIALIZING,           "initAction");
        addAction(GEMStates.CONFIGURING,            "configureAction");
        addAction(GEMStates.HALTING,                "haltAction");
        addAction(GEMStates.PREPARING_TTSTEST_MODE, "preparingTTSTestModeAction");
        addAction(GEMStates.TESTING_TTS,            "testingTTSAction");
        addAction(GEMStates.COLDRESETTING,          "coldResettingAction");
        addAction(GEMStates.PAUSING,                "pauseAction");
        addAction(GEMStates.RECOVERING,             "recoverAction");
        addAction(GEMStates.RESETTING,              "resetAction");
        addAction(GEMStates.RESUMING,               "resumeAction");
        addAction(GEMStates.STARTING,               "startAction");
        addAction(GEMStates.STOPPING,               "stopAction");

        addAction(GEMStates.FIXINGSOFTERROR,        "fixSoftErrorAction");

        addAction(GEMStates.RUNNINGDEGRADED,          "runningDegradedAction");           // for testing with external inputs
        addAction(GEMStates.RUNNINGSOFTERRORDETECTED, "runningSoftErrorDetectedAction");  // for testing with external inputs
        addAction(GEMStates.RUNNING,                  "runningAction");                   // for testing with external inputs
    }


    public void init()
        throws rcms.fm.fw.EventHandlerException
    {
        functionManager = (GEMFunctionManager) getUserFunctionManager();
        qualifiedGroup  = functionManager.getQualifiedGroup();

        // debug
        logger.debug("init() called: functionManager=" + functionManager );
    }


    // get official CMS run and sequence number
    protected RunNumberData getOfficialRunNumber() {
        // check availability of runInfo DB
        RunInfoConnectorIF ric = functionManager.getRunInfoConnector();
        // Get SID from parameter
        Sid = ((IntegerT)functionManager.getParameterSet().get(GEMParameters.SID).getValue()).getInteger();
        if ( ric == null ) {
            logger.error("[GEM FM::" + functionManager.FMname + "] RunInfoConnector is empty i.e. Is there a RunInfo DB or is it down?");

            // by default give run number 0
            return new RunNumberData(new Integer(Sid),new Integer(0),functionManager.getOwner(),Calendar.getInstance().getTime());
        } else {
            RunSequenceNumber rsn = new RunSequenceNumber(ric,functionManager.getOwner(),RunSequenceName);
            RunNumberData rnd = rsn.createRunSequenceNumber(Sid);

            logger.info("[GEM FM::" + functionManager.FMname + "] received run number: " + rnd.getRunNumber()
                        + " and sequence number: " + rnd.getSequenceNumber());

            functionManager.GEMRunInfo = null; // make RunInfo ready for the next round of run info to store
            return rnd;
        }
    }

    // establish connection to RunInfoDB - if needed
    protected void checkRunInfoDBConnection()
    {
        if (functionManager.GEMRunInfo == null) {
            logger.info("[GEM FM::" + functionManager.FMname + "] creating new RunInfo accessor with namespace: "
                        + functionManager.GEM_NS + " now ...");

            //Get SID from parameter
            Sid = ((IntegerT)functionManager.getParameterSet().get(GEMParameters.SID).getValue()).getInteger();

            RunInfoConnectorIF ric = functionManager.getRunInfoConnector();
            functionManager.GEMRunInfo =  new RunInfo(ric,Sid,Integer.valueOf(functionManager.RunNumber));

            functionManager.GEMRunInfo.setNameSpace(functionManager.GEM_NS);

            logger.info("[GEM FM::" + functionManager.FMname + "] ... RunInfo accessor available.");
        }
    }

    // class which makes the GEMINI fun messages
    protected class MoveTheGEMINI {

        private Boolean movehimtotheright = true;
        private Integer moves = 0;
        private Integer offset = 0;
        private Integer maxmoves = 30;
        private String TheGEMINI ="♊";
        private String TheLine = "";
        private Random theDice;

        public MoveTheGEMINI(Integer themaxmoves) {
            movehimtotheright = true;
            moves = 0;
            offset = 0;
            maxmoves = themaxmoves;
            if (maxmoves < 30) { maxmoves = 30; }
            TheLine = "";
            theDice = new Random();
            logger.debug("[GEM FM::" + functionManager.FMname + "] The GEMINI should show up - Look at it as it moves through the sky");
        }

        public void movehim()
        {
            TheLine = "";
            if (movehimtotheright) {
                moves++;
                TheLine +="_";
                for (int count=1; count < moves; count++) {
                    Integer starit = theDice.nextInt(10);
                    if (starit < 9) { TheLine +="_"; }
                    else { TheLine +="★"; }
                }
                TheLine += TheGEMINI;

                if ((maxmoves-moves) > 6) {
                    Integer sayit = theDice.nextInt(10);
                    if (sayit == 9) {
                        Integer saywhat = theDice.nextInt(10);
                        if (saywhat >= 0 && saywhat <= 4) {
                            TheLine += " GEMINI calling Earth!";
                            offset = 22;
                        }
                        else if (saywhat == 5 && (maxmoves-moves) > 25) {
                            TheLine += " Swimming in dark matter!";
                            offset = 25;
                        }
                        else if (saywhat == 6 && (maxmoves-moves) > 12) {
                            TheLine += " Where am I?";
                            offset = 12;
                        }
                        else if (saywhat == 7 && (maxmoves-moves) > 20) {
                            TheLine += " Diving outer space";
                            offset = 20;
                        }
                        else if (saywhat == 8 && (maxmoves-moves) > 18) {
                            TheLine += " I have two faces!";
                            offset = 18;
                        }
                        else {
                            TheLine += " Hi!";
                            offset = 4;
                        }
                    }
                }

                for (int count=moves+offset; count < maxmoves; count++) { TheLine +="_"; }
                offset = 0;
                TheLine +="_";
                if (moves==maxmoves) {
                    movehimtotheright = false;
                }
                else {
                    Integer wheretogo = theDice.nextInt(10);
                    if (wheretogo >= 7) {
                        movehimtotheright = false;
                    }
                }
            }
            else {
                TheLine +="_";
                for (int count=moves; count > 1; count--) { TheLine +="_"; }
                TheLine += TheGEMINI;
                for (int count=maxmoves; count > moves; count--) { TheLine +="_"; }
                TheLine +="_";
                moves--;
                if (moves<1) {
                    movehimtotheright = true;
                    moves = 0;
                }
                else {
                    Integer wheretogo = theDice.nextInt(10);
                    if (wheretogo >= 7) {
                        movehimtotheright = true;
                    }
                }
            }
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(TheLine)));
        }
    }

    @SuppressWarnings("unchecked") // SHOULD REALLY MAKE SURE THAT THIS IS NECESSARY AND NOT JUST DUE TO BAD JAVA
        public void initAction(Object obj)
        throws UserActionException
    {
        if (obj instanceof StateNotification) {
            // triggered by State Notification from child resource

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            logger.info("[GEM FM::" + functionManager.FMname + "] Recieved Initialize state notification");

            return;
        } else if (obj instanceof StateEnteredEvent) {
            // triggered by entered state action
            // let's command the child resources

            // debug
            logger.debug("initAction called.");

            // set action
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                                        new StringT("Initialize called")));
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                                        new StringT("Initializing")));

            // get the parameters of the command
            // Integer sid;
            String globalConfKey = null;

            try {
                // ParameterSet<CommandParameter> parameterSet = getUserFunctionManager().getLastInput().getParameterSet();
                ParameterSet parameterSet = functionManager.getParameterSet();
                if (parameterSet.get(GEMParameters.SID) != null) {
                    Sid = ((IntegerT)parameterSet.get(GEMParameters.SID).getValue()).getInteger();
                    ((FunctionManagerParameter<IntegerT>)functionManager.getParameterSet()
                     .get(GEMParameters.INITIALIZED_WITH_SID))
                        .setValue(new IntegerT(Sid));
                    logger.info("[GEM FM::" + functionManager.FMname + "] INITIALIZED_WITH_SID has been set");
                    /*
                    // For the moment this parameter is only here to show if it is correctly set after initialization
                    // -> Really needed in future?
                    getParameterSet().get("INITIALIZED_WITH_SID").setValue(new IntegerT(sid));
                    */
                } else {
                    logger.warn("[GEM FM::" + functionManager.FMname + "] SID has been found to be null");
                }
                // globalConfKey = ((CommandParameter<StringT>)parameterSet.get(GEMParameters.GLOBAL_CONF_KEY)).getValue().toString();
            } catch (Exception e) {
                // go to error, we require parameters
                String msg = "initAction: error reading command parameters of Initialize command.";
                logger.error(msg, e);
                // notify error
                sendCMSError(msg);
                //go to error state
                functionManager.fireEvent( GEMInputs.SETERROR );
                return;
            }

            QualifiedGroup qg = functionManager.getQualifiedGroup();
            VectorT<StringT> availableResources = new VectorT<StringT>();

            List<QualifiedResource> qrList = qg.seekQualifiedResourcesOfType(new FunctionManager());
            for (QualifiedResource qr : qrList) {
                logger.info("[GEM FM::" + functionManager.FMname + "]  function manager resource found: " + qr.getName());
                availableResources.add(new StringT(qr.getName()));
            }

            qrList = qg.seekQualifiedResourcesOfType(new XdaqExecutive());
            for (QualifiedResource qr : qrList) {
                logger.info("[GEM FM::" + functionManager.FMname + "]  xdaq executive resource found: " + qr.getName());
                availableResources.add(new StringT(qr.getName()));
                // Snippet to get XDAQExecutive xml config
                XdaqExecutive exec = (XdaqExecutive)qr;
                XdaqExecutiveConfiguration config =  exec.getXdaqExecutiveConfiguration();
                String ExecXML = config.getXml();
                String EnvironmentLoaded = config.getEnvironmentString();
                // functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("XML_Executive",new StringT(ExecXML)));
                functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("Environment_settings",
                                                                                            new StringT(EnvironmentLoaded)));
                logger.info("Executive config "+ ExecXML);
                logger.info("Environment settings "+ EnvironmentLoaded);
            }

            // Looking for job control resources
            qrList = qg.seekQualifiedResourcesOfType(new JobControl());
            // logger.info("[GEM FM::" + functionManager.FMname + "] Looking for job control resources");
            for (QualifiedResource qr : qrList) {
                logger.info("[GEM FM::" + functionManager.FMname + "]  job control resource found: " + qr.getName());
                availableResources.add(new StringT(qr.getName()));
                JobControl JC = (JobControl)qr;
                // JC.executeCommand(); // BUG!!! executeCommand is not a function//
                // Snippet to get JobControl xml config - not sure it is possible to have this....
                /*
                JobControl JC = (JobControl)qr;
                XdaqExecutiveConfiguration JCconfig =  JC.getXdaqExecutiveConfiguration();
                String JCXML = JCconfig.getXml();
                logger.info("JobControl config "+ JCXML);
                */
            }

            qrList = qg.seekQualifiedResourcesOfType(new XdaqApplication());
            for (QualifiedResource qr : qrList) {
                logger.info("[GEM FM::" + functionManager.FMname + "]  xdaq application resource found: " + qr.getName());
                availableResources.add(new StringT(qr.getName()));
            }

            functionManager.getParameterSet().put(new FunctionManagerParameter<VectorT<StringT>>("AVAILABLE_RESOURCES",
                                                                                                 availableResources));

            // initialize all XDAQ executives
            logger.info("[GEM FM::" + functionManager.FMname + "] calling initXDAQ");
            initXDAQ();
            logger.info("[GEM FM::" + functionManager.FMname + "] initXDAQ finished");

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            // force TCDS HALTED
            if (functionManager.containerTCDSControllers != null) {
                if (!functionManager.containerTCDSControllers.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to halt TCDS on initialize.");
                        functionManager.haltTCDSControllers();
                    } catch (UserActionException e) {
                        String msg = "[GEM FM::" + functionManager.FMname + "] initAction: ";
                        functionManager.goToError(msg,e);
                        // functionManager.sendCMSError(msg);  // when do this rather than goToError, or both?
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                        logger.error(msg);
                    }
                }
            }

            // initialize GEMFSMApplications
            if (functionManager.containerGEMSupervisor != null) {
                if (!functionManager.containerGEMSupervisor.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to initialize GEMSupervisor.");
                        functionManager.containerGEMSupervisor.execute(GEMInputs.INITIALIZE);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "[GEM FM::" + functionManager.FMname + "] initAction: ";
                        functionManager.goToError(msg,e);
                        // functionManager.sendCMSError(msg);  // when do this rather than goToError, or both?
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                        logger.error(msg);
                    }
                }
            }

            // ? ferol/EVM/BU/RU?
            if (functionManager.containerFEDKIT != null) {
                if (!functionManager.containerFEDKIT.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to halt uFEDKIT resources on initialize.");
                        functionManager.containerFEDKIT.execute(GEMInputs.HALT);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "[GEM FM::" + functionManager.FMname + "] initAction: ";
                        functionManager.goToError(msg,e);
                        // functionManager.sendCMSError(msg);  // when do this rather than goToError, or both?
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",     new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                        logger.error(msg);
                    }
                }
            }

            /*
            // set exported parameters
            ((FunctionManagerParameter<IntegerT>)functionManager.getParameterSet()
             .get(GEMParameters.INITIALIZED_WITH_SID))
                .setValue(new IntegerT(Sid));
            ((FunctionManagerParameter<StringT>)functionManager.getParameterSet()
             .get(GEMParameters.INITIALIZED_WITH_GLOBAL_CONF_KEY))
                .setValue(new StringT(globalConfKey));
            */

            // go to HALT
            functionManager.fireEvent( GEMInputs.SETHALTED );

            // set action
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                                        new StringT("")));
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                                        new StringT("Initialized -> Halted")));

            logger.info("initAction Executed");
        }
    }


    public void resetAction(Object obj)
        throws UserActionException
    {
        if (obj instanceof StateNotification) {
            // triggered by State Notification from child resource

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            logger.info("[GEM FM::" + functionManager.FMname + "] Recieved Reset state notification");

            return;
        } else if (obj instanceof StateEnteredEvent) {
            // triggered by entered state action
            // let's command the child resources

            // debug
            logger.debug("resetAction called.");

            // set action
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                                        new StringT("Reset called")));
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                                        new StringT("Resetting")));

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            // force TCDS HALTED
            if (functionManager.containerTCDSControllers != null) {
                if (!functionManager.containerTCDSControllers.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to halt TCDS on reset.");
                        functionManager.haltTCDSControllers();
                    } catch (UserActionException e) {
                        String errMsg = "[GEM FM::" + functionManager.FMname + "] resetAction: " + e.getMessage();
                        logger.warn(errMsg);
                    }
                }
            }

            // reset GEMFSMApplications
            if (functionManager.containerGEMSupervisor != null) {
                if (!functionManager.containerGEMSupervisor.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to reset GEMSupervisor.");
                        functionManager.containerGEMSupervisor.execute(GEMInputs.RESET);
                    } catch (QualifiedResourceContainerException e) {
                        String errMsg = "[GEM FM::" + functionManager.FMname + "] resetAction: " + e.getMessage();
                        logger.warn(errMsg);
                    }
                }
            }

            // ? ferol/EVM/BU/RU?
            if (functionManager.containerFEDKIT != null) {
                if (!functionManager.containerFEDKIT.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to halt uFEDKIT resources on reset.");
                        functionManager.containerFEDKIT.execute(GEMInputs.HALT);
                    } catch (QualifiedResourceContainerException e) {
                        String errMsg = "[GEM FM::" + functionManager.FMname + "] resetAction: " + e.getMessage();
                        logger.warn(errMsg);
                    }
                }
            }

            functionManager.GEMRunInfo = null; // make RunInfo ready for the next round of run info to store

            // go to Initital
            functionManager.fireEvent( GEMInputs.SETHALTED );

            // Clean-up of the Function Manager parameters
            cleanUpFMParameters();

            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                                        new StringT("")));
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                                        new StringT("Reset - Halted")));

            logger.info("resetAction Executed");
        }
    }

    public void recoverAction(Object obj)
        throws UserActionException
    {
        if (obj instanceof StateNotification) {
            // triggered by State Notification from child resource

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            logger.info("[GEM FM::" + functionManager.FMname + "] Recieved Recover state notification");

            return;
        } else if (obj instanceof StateEnteredEvent) {

            System.out.println("Executing recoverAction");
            logger.info("Executing recoverAction");

            // set action
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("recovering")));

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/
            // force TCDS HALTED
            // force GEMFSMApplications HALTED
            // force ferol/EVM/BU/RU HALTED?

            // leave intermediate state
            functionManager.fireEvent( GEMInputs.SETHALTED );

            // Clean-up of the Function Manager parameters
            cleanUpFMParameters();

            logger.info("recoverAction Executed");
        }
    }

    @SuppressWarnings("unchecked") // SHOULD REALLY MAKE SURE THAT THIS IS NECESSARY AND NOT JUST DUE TO BAD JAVA
        public void configureAction(Object obj)
        throws UserActionException
    {
        if (obj instanceof StateNotification) {
            // triggered by State Notification from child resource

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            logger.info("[GEM FM::" + functionManager.FMname + "] Recieved Configure state notification");

            return;
        } else if (obj instanceof StateEnteredEvent) {
            System.out.println("Executing configureAction");
            logger.info("Executing configureAction");

            // set action
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                                        new StringT("Configure action called")));
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                                        new StringT("Configuring")));

            // get the parameters of the command
            Integer runNumber     = -1;
            String  runKey        = "not set";
            String  fedEnableMask = "not set";

            ParameterSet<CommandParameter> parameterSet = getUserFunctionManager().getLastInput().getParameterSet();
            if (parameterSet.size() == 0)  {
                logger.info("[GEM FM::" + functionManager.FMname + "] parameterSet is empty");
            }

            try {
                runNumber     = ((CommandParameter<IntegerT>)parameterSet.get(GEMParameters.RUN_NUMBER)).getValue().getInteger();
                runKey        = ((CommandParameter<StringT>)parameterSet.get(GEMParameters.RUN_KEY)).getValue().toString();
                fedEnableMask = ((CommandParameter<StringT>)parameterSet.get(GEMParameters.FED_ENABLE_MASK)).getValue().toString();

                /*
                runNumber     = ((IntegerT)parameterSet.get(GEMParameters.RUN_NUMBER).getValue()).getInteger();
                runKey        = ((StringT)parameterSet.get(GEMParameters.RUN_KEY).getValue()).toString();
                fedEnableMask = ((StringT)parameterSet.get(GEMParameters.FED_ENABLE_MASK).getValue()).toString();
                */
            } catch (Exception e) {
                // go to error, we require parameters
                String errMsg = "configureAction: error reading command parameters of Configure command.";
                logger.error(errMsg, e);
                sendCMSError(errMsg);
                //go to error state
                functionManager.fireEvent( GEMInputs.SETERROR );
                return;
            }

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            // Set the configuration parameters in the Function Manager parameters
            ((FunctionManagerParameter<IntegerT>)functionManager.getParameterSet()
             .get(GEMParameters.CONFIGURED_WITH_RUN_NUMBER))
                .setValue(new IntegerT(runNumber));
            ((FunctionManagerParameter<StringT>)functionManager.getParameterSet()
             .get(GEMParameters.CONFIGURED_WITH_RUN_KEY))
                .setValue(new StringT(runKey));
            ((FunctionManagerParameter<StringT>)functionManager.getParameterSet()
             .get(GEMParameters.CONFIGURED_WITH_FED_ENABLE_MASK))
                .setValue(new StringT(fedEnableMask));

            // configure TCDS (LPM then ICI then PI)
            if (functionManager.containerTCDSControllers != null) {
                if (!functionManager.containerTCDSControllers.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to configure TCDS on configure.");
                        functionManager.configureTCDSControllers();
                    } catch (UserActionException e) {
                        String errMsg = "[GEM FM::" + functionManager.FMname + "] configureAction: " + e.getMessage();
                        logger.error(errMsg);
                        //functionManager.sendCMSError(msg);
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(errMsg)));
                    }
                }
            }

            // configure GEMFSMApplications
            if (functionManager.containerGEMSupervisor != null) {
                if (!functionManager.containerGEMSupervisor.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to configure GEMSupervisor.");
                        functionManager.containerGEMSupervisor.execute(GEMInputs.CONFIGURE);
                    } catch (QualifiedResourceContainerException e) {
                        String errMsg = "[GEM FM::" + functionManager.FMname + "] configureAction: " + e.getMessage();
                        logger.error(errMsg);
                        //functionManager.sendCMSError(msg);
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(errMsg)));
                    }
                }
            }

            // configure ferol/EVM/BU/RU?
            // need to send the FED_ENABLE_MASK to the EVM and BU
            // need to configure first the EVM then BU and then the FerolController
            /*
            if (functionManager.containerFEDKIT != null) {
                if (!functionManager.containerFEDKIT.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to configure uFEDKIT resources on configure.");
                        functionManager.containerFEDKIT.execute(GEMInputs.CONFIGURE);
                    } catch (QualifiedResourceContainerException e) {
                        String errMsg = "[GEM FM::" + functionManager.FMname + "] configureAction: " + e.getMessage();
                        logger.error(errMsg);
                        //functionManager.sendCMSError(msg);
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(errMsg)));
                    }
                }
            }
            */

            if (functionManager.containerEVM != null) {
                if (!functionManager.containerEVM.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to configure EVM resources on configure.");
                        functionManager.containerEVM.execute(GEMInputs.CONFIGURE);
                    } catch (QualifiedResourceContainerException e) {
                        String errMsg = "[GEM FM::" + functionManager.FMname + "] configureAction: " + e.getMessage();
                        logger.error(errMsg);
                        //functionManager.sendCMSError(msg);
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(errMsg)));
                    }
                }
            }

            if (functionManager.containerBU != null) {
                if (!functionManager.containerBU.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to configure BU resources on configure.");
                        functionManager.containerBU.execute(GEMInputs.CONFIGURE);
                    } catch (QualifiedResourceContainerException e) {
                        String errMsg = "[GEM FM::" + functionManager.FMname + "] configureAction: " + e.getMessage();
                        logger.error(errMsg);
                        //functionManager.sendCMSError(msg);
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(errMsg)));
                    }
                }
            }

            if (functionManager.containerFerol != null) {
                if (!functionManager.containerFerol.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to configure Ferol resources on configure.");
                        functionManager.containerFerol.execute(GEMInputs.CONFIGURE);
                    } catch (QualifiedResourceContainerException e) {
                        String errMsg = "[GEM FM::" + functionManager.FMname + "] configureAction: " + e.getMessage();
                        logger.error(errMsg);
                        //functionManager.sendCMSError(msg);
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(errMsg)));
                    }
                }
            }

            // leave intermediate state
            functionManager.fireEvent( GEMInputs.SETCONFIGURED );

            // set action
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("")));
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,new StringT("Configured")));

            logger.info("configureAction Executed");
        }
    }

    public void startAction(Object obj)
        throws UserActionException
    {
        if (obj instanceof StateNotification) {
            // triggered by State Notification from child resource

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/
            // START GEMFSMApplications
            // ENABLE? ferol/EVM/BU/RU?
            // START TCDS

            return;
        } else if (obj instanceof StateEnteredEvent) {
            System.out.println("Executing startAction");
            logger.info("Executing startAction");

            // set action
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                                        new StringT("Started action called!")));
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                                        new StringT("Starting!!!")));

            // get the parameters of the command
            ParameterSet<CommandParameter> parameterSet = getUserFunctionManager().getLastInput().getParameterSet();

            if (functionManager.getRunInfoConnector() != null) {
                RunNumberData rnd = getOfficialRunNumber();

                functionManager.RunNumber = rnd.getRunNumber();
                RunSeqNumber              = rnd.getSequenceNumber();

                functionManager.getParameterSet().put(new FunctionManagerParameter<IntegerT>("RUN_NUMBER",
                                                                                             new IntegerT(functionManager.RunNumber)));
                functionManager.getParameterSet().put(new FunctionManagerParameter<IntegerT>("RUN_SEQ_NUMBER",
                                                                                             new IntegerT(RunSeqNumber)));
                logger.info("[GEM LVL1 " + functionManager.FMname + "] ... run number: " + functionManager.RunNumber
                            + ", SequenceNumber: " + RunSeqNumber);
            } else {
                logger.error("[GEM LVL1 " + functionManager.FMname + "] Official RunNumber requested, but cannot establish "
                             +  "RunInfo Connection. Is there a RunInfo DB? or is RunInfo DB down?");
                logger.info("[GEM LVL1 " + functionManager.FMname + "] Going to use run number =" + functionManager.RunNumber
                            +  ", RunSeqNumber = " +  RunSeqNumber);
            }

            // check parameter set
            /*if (parameterSet.size()==0 || parameterSet.get(GEMParameters.RUN_NUMBER) == null )  {

            // go to error, we require parameters
            String errMsg = "startAction: no parameters given with start command.";

            // log error
            logger.error(errMsg);

            // notify error
            sendCMSError(errMsg);

            // go to error state
            functionManager.fireEvent( GEMInputs.SETERROR );
            return;
            }*/

            // get the run number from the start command
            Integer runNumber = ((IntegerT)parameterSet.get(GEMParameters.RUN_NUMBER).getValue()).getInteger();

            functionManager.getParameterSet().put(new FunctionManagerParameter<IntegerT>(GEMParameters.STARTED_WITH_RUN_NUMBER,
                                                                                         new IntegerT(runNumber)));

            // Set the run number in the Function Manager parameters
            functionManager.getParameterSet().put(new FunctionManagerParameter<IntegerT>(GEMParameters.RUN_NUMBER,
                                                                                         new IntegerT(runNumber)));

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/
            /*
            // Set the run number in the Function Manager parameters
            ((FunctionManagerParameter<IntegerT>)functionManager.getParameterSet()
             .get(GEMParameters.STARTED_WITH_RUN_NUMBER))
                .setValue(new IntegerT(runNumber));
            */

            // START GEMFSMApplications
            if (functionManager.containerGEMSupervisor != null) {
                if (!functionManager.containerGEMSupervisor.isEmpty()) {
                    XDAQParameter pam = null;
                    // prepare and set for all GEM supervisors the RunType
                    for (QualifiedResource qr : functionManager.containerGEMSupervisor.getApplications() ){
                        try {
                            pam = ((XdaqApplication)qr).getXDAQParameter();
                            pam.select(new String[] {"RunNumber"});
                            pam.setValue("RunNumber",functionManager.RunNumber.toString());
                            pam.send();
                        } catch (XDAQTimeoutException e) {
                            String msg = "[GEM FM::" + functionManager.FMname + "] Error! XDAQTimeoutException: startAction() when "
                                + " trying to send the functionManager.RunNumber to the GEM supervisor\n Perhaps this "
                                + "application is dead!?";
                            functionManager.goToError(msg,e);
                            logger.error(msg);
                        } catch (XDAQException e) {
                            String msg = "[GEM FM::" + functionManager.FMname + "] Error! XDAQException: startAction() when trying "
                                + "to send the functionManager.RunNumber to the GEM supervisor";
                            functionManager.goToError(msg,e);
                            logger.error(msg);
                        }
                    }

                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to start GEMSupervisor.");
                        functionManager.containerGEMSupervisor.execute(GEMInputs.START);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "[GEM FM::" + functionManager.FMname + "] startAction: " + e.getMessage();
                        logger.error(msg);
                        //functionManager.sendCMSError(msg);
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                    }
                }
            }

            // ENABLE? ferol/EVM/BU/RU?
            // need to start first the EVM then BU and then the FerolController
            /*
            if (functionManager.containerFEDKIT != null) {
                if (!functionManager.containerFEDKIT.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to enable uFEDKIT resources on start.");
                        functionManager.containerFEDKIT.execute(GEMInputs.ENABLE);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "[GEM FM::" + functionManager.FMname + "] startAction: " + e.getMessage();
                        logger.error(msg);
                        functionManager.goToError(msg,e);
                        // functionManager.sendCMSError(msg);  // when do this rather than goToError, or both?
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                    }
                }
            }
            */

            if (functionManager.containerEVM != null) {
                if (!functionManager.containerEVM.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to enable EVM resources on start.");
                        functionManager.containerEVM.execute(GEMInputs.ENABLE);
                    } catch (QualifiedResourceContainerException e) {
                        String errMsg = "[GEM FM::" + functionManager.FMname + "] startAction: " + e.getMessage();
                        logger.error(errMsg);
                        //functionManager.sendCMSError(msg);
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(errMsg)));
                    }
                }
            }

            if (functionManager.containerBU != null) {
                if (!functionManager.containerBU.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to enable BU resources on start.");
                        functionManager.containerBU.execute(GEMInputs.ENABLE);
                    } catch (QualifiedResourceContainerException e) {
                        String errMsg = "[GEM FM::" + functionManager.FMname + "] startAction: " + e.getMessage();
                        logger.error(errMsg);
                        //functionManager.sendCMSError(msg);
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(errMsg)));
                    }
                }
            }

            if (functionManager.containerFerol != null) {
                if (!functionManager.containerFerol.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to enable Ferol resources on start.");
                        functionManager.containerFerol.execute(GEMInputs.ENABLE);
                    } catch (QualifiedResourceContainerException e) {
                        String errMsg = "[GEM FM::" + functionManager.FMname + "] startAction: " + e.getMessage();
                        logger.error(errMsg);
                        //functionManager.sendCMSError(msg);
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(errMsg)));
                    }
                }
            }

            // ENABLE TCDS
            if (functionManager.containerTCDSControllers != null) {
                if (!functionManager.containerTCDSControllers.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to enable TCDS resources on start.");
                        functionManager.enableTCDSControllers();
                    } catch (UserActionException e) {
                        String msg = "[GEM FM::" + functionManager.FMname + "] startAction: " + e.getMessage();
                        logger.error(msg);
                        functionManager.goToError(msg,e);
                        // functionManager.sendCMSError(msg);  // when do this rather than goToError, or both?
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                    }
                }
            }

            // leave intermediate state
            functionManager.fireEvent( GEMInputs.SETRUNNING );

            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                                        new StringT("")));
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                                        new StringT("Running!!!")));
            logger.debug("startAction Executed");
        }
    }

    public void pauseAction(Object obj)
        throws UserActionException
    {
        if (obj instanceof StateNotification) {
            // triggered by State Notification from child resource

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/
            // PAUSE TCDS
            // PAUSE GEMFSMApplications
            // PAUSE ferol/EVM/BU/RU?

            return;
        }

        else if (obj instanceof StateEnteredEvent) {

            System.out.println("Executing pauseAction");
            logger.info("Executing pauseAction");

            // set action
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                                        new StringT("Pause action issued")));
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                                        new StringT("Pausing")));

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            // pause TCDS
            if (functionManager.containerTCDSControllers != null) {
                if (!functionManager.containerTCDSControllers.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to pause TCDS on pause.");
                        functionManager.pauseTCDSControllers();
                    } catch (UserActionException e) {
                        String msg = "[GEM FM::" + functionManager.FMname + "] pauseAction: " + e.getMessage();
                        functionManager.goToError(msg,e);
                        // functionManager.sendCMSError(msg);  // when do this rather than goToError, or both?
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                        logger.error(msg);
                    }
                }
            }

            // pause GEMFSMApplications
            if (functionManager.containerGEMSupervisor != null) {
                if (!functionManager.containerGEMSupervisor.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to pause GEMSupervisor.");
                        functionManager.containerGEMSupervisor.execute(GEMInputs.PAUSE);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "[GEM FM::" + functionManager.FMname + "] pauseAction: " + e.getMessage();
                        functionManager.goToError(msg,e);
                        // functionManager.sendCMSError(msg);  // when do this rather than goToError, or both?
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                        logger.error(msg);
                    }
                }
            }

            /*
            // PAUSE ferol/EVM/BU/RU?
            if (functionManager.containerFEDKIT != null) {
                if (!functionManager.containerFEDKIT.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to pause uFEDKIT resources on pause.");
                        functionManager.containerFEDKIT.execute(GEMInputs.PAUSE);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "[GEM FM::" + functionManager.FMname + "] pauseAction: " + e.getMessage();
                        functionManager.goToError(msg,e);
                        // functionManager.sendCMSError(msg);  // when do this rather than goToError, or both?
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                        logger.error(msg);
                    }
                }
            }
            */

            // leave intermediate state
            functionManager.fireEvent( GEMInputs.SETPAUSED );

            // set action
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("")));
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,new StringT("Paused")));

            logger.debug("pausingAction Executed");
        }
    }

    public void stopAction(Object obj)
        throws UserActionException
    {
        if (obj instanceof StateNotification) {
            // triggered by State Notification from child resource

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/
            // STOP TCDS
            // STOP GEMFSMApplications
            // STOP ferol/EVM/BU/RU?

            return;
        }

        else if (obj instanceof StateEnteredEvent) {
            System.out.println("Executing stopAction");
            logger.info("Executing stopAction");

            // set action
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                                        new StringT("Stop requested")));
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                                        new StringT("Stopping")));

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            // stop TCDS
            if (functionManager.containerTCDSControllers != null) {
                if (!functionManager.containerTCDSControllers.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to stop TCDS on stop.");
                        functionManager.stopTCDSControllers();
                    } catch (UserActionException e) {
                        String msg = "[GEM FM::" + functionManager.FMname + "] stopAction: " + e.getMessage();
                        functionManager.goToError(msg,e);
                        // functionManager.sendCMSError(msg);  // when do this rather than goToError, or both?
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                        logger.error(msg);
                    }
                }
            }

            // stop GEMFSMApplications
            if (functionManager.containerGEMSupervisor != null) {
                if (!functionManager.containerGEMSupervisor.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to stop GEMSupervisor.");
                        functionManager.containerGEMSupervisor.execute(GEMInputs.STOP);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "[GEM FM::" + functionManager.FMname + "] stopAction: " + e.getMessage();
                        functionManager.goToError(msg,e);
                        // functionManager.sendCMSError(msg);  // when do this rather than goToError, or both?
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                        logger.error(msg);
                    }
                }
            }

            // ? ferol/EVM/BU/RU?
            // stop ferol then BU then EVM
            /*
            if (functionManager.containerFEDKIT != null) {
                if (!functionManager.containerFEDKIT.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to stop uFEDKIT resources on stop.");
                        functionManager.containerFEDKIT.execute(GEMInputs.STOP);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "[GEM FM::" + functionManager.FMname + "] stopAction: " + e.getMessage();
                        functionManager.goToError(msg,e);
                        // functionManager.sendCMSError(msg);  // when do this rather than goToError, or both?
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                        logger.error(msg);
                    }
                }
            }
            */

            if (functionManager.containerEVM != null) {
                if (!functionManager.containerEVM.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to stop EVM resources on stop.");
                        functionManager.containerEVM.execute(GEMInputs.STOP);
                    } catch (QualifiedResourceContainerException e) {
                        String errMsg = "[GEM FM::" + functionManager.FMname + "] stopAction: " + e.getMessage();
                        logger.error(errMsg);
                        //functionManager.sendCMSError(msg);
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(errMsg)));
                    }
                }
            }
            
            if (functionManager.containerBU != null) {
                if (!functionManager.containerBU.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to stop BU resources on stop.");
                        functionManager.containerBU.execute(GEMInputs.STOP);
                    } catch (QualifiedResourceContainerException e) {
                        String errMsg = "[GEM FM::" + functionManager.FMname + "] stopAction: " + e.getMessage();
                        logger.error(errMsg);
                        //functionManager.sendCMSError(msg);
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(errMsg)));
                    }
                }
            }

            if (functionManager.containerFerol != null) {
                if (!functionManager.containerFerol.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to stop Ferol resources on stop.");
                        functionManager.containerFerol.execute(GEMInputs.STOP);
                    } catch (QualifiedResourceContainerException e) {
                        String errMsg = "[GEM FM::" + functionManager.FMname + "] stopAction: " + e.getMessage();
                        logger.error(errMsg);
                        //functionManager.sendCMSError(msg);
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(errMsg)));
                    }
                }
            }            

            // leave intermediate state
            functionManager.fireEvent( GEMInputs.SETCONFIGURED );

            // set action
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                                        new StringT("")));
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                                        new StringT("Stopping - Configured")));

            logger.debug("stopAction Executed");
        }
    }

    public void resumeAction(Object obj)
        throws UserActionException
    {
        if (obj instanceof StateNotification) {
            // triggered by State Notification from child resource

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/
            // RESUME ferol/EVM/BU/RU?
            // RESUME GEMFSMApplications
            // RESUME TCDS

            return;
        } else if (obj instanceof StateEnteredEvent) {
            System.out.println("Executing resumeAction");
            logger.info("Executing resumeAction");

            // set action
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                                        new StringT("Resume called")));
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                                        new StringT("Resuming")));

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            if (functionManager.containerGEMSupervisor != null) {
                if (!functionManager.containerGEMSupervisor.isEmpty()) {
                    XDAQParameter pam = null;
                    // prepare and set for all GEM supervisors the RunType
                    for (QualifiedResource qr : functionManager.containerGEMSupervisor.getApplications() ){
                        try {
                            pam = ((XdaqApplication)qr).getXDAQParameter();
                            pam.select(new String[] {"RunNumber"});
                            pam.setValue("RunNumber",functionManager.RunNumber.toString());
                            pam.send();
                        } catch (XDAQTimeoutException e) {
                            String msg = "[GEM FM::" + functionManager.FMname + "] Error! XDAQTimeoutException: startAction() when "
                                + " trying to send the functionManager.RunNumber to the GEM supervisor\n Perhaps this "
                                + "application is dead!?";
                            functionManager.goToError(msg,e);
                            logger.error(msg);
                        } catch (XDAQException e) {
                            String msg = "[GEM FM::" + functionManager.FMname + "] Error! XDAQException: startAction() when trying "
                                + "to send the functionManager.RunNumber to the GEM supervisor";
                            functionManager.goToError(msg,e);
                            logger.error(msg);
                        }
                    }

                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to start GEMSupervisor.");
                        functionManager.containerGEMSupervisor.execute(GEMInputs.START);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "[GEM FM::" + functionManager.FMname + "] startAction: " + e.getMessage();
                        logger.error(msg);
                        //functionManager.sendCMSError(msg);
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                    }
                }
            }

            /*
            // RESUME? ferol/EVM/BU/RU?
            // need to resume first the EVM then BU and then the FerolController
            if (functionManager.containerFEDKIT != null) {
                if (!functionManager.containerFEDKIT.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to enable uFEDKIT resources on start.");
                        functionManager.containerFEDKIT.execute(GEMInputs.ENABLE);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "[GEM FM::" + functionManager.FMname + "] startAction: " + e.getMessage();
                        logger.error(msg);
                        functionManager.goToError(msg,e);
                        // functionManager.sendCMSError(msg);  // when do this rather than goToError, or both?
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                    }
                }
            }
            */

            // RESUME TCDS
            if (functionManager.containerTCDSControllers != null) {
                if (!functionManager.containerTCDSControllers.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to enable TCDS resources on resume.");
                        functionManager.enableTCDSControllers();
                    } catch (UserActionException e) {
                        String msg = "[GEM FM::" + functionManager.FMname + "] resumeAction: " + e.getMessage();
                        logger.error(msg);
                        functionManager.goToError(msg,e);
                        // functionManager.sendCMSError(msg);  // when do this rather than goToError, or both?
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                    }
                }
            }

            // leave intermediate state
            functionManager.fireEvent( functionManager.hasSoftError() ? GEMInputs.SETRESUMEDSOFTERRORDETECTED :
                                       ( functionManager.isDegraded() ? GEMInputs.SETRESUMEDDEGRADED : GEMInputs.SETRESUMED ));


            // Clean-up of the Function Manager parameters
            cleanUpFMParameters();

            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                                        new StringT("")));
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                                        new StringT("Resuming - Running")));

            logger.debug("resumeAction Executed");
        }
    }

    public void haltAction(Object obj)
        throws UserActionException
    {
        if (obj instanceof StateNotification) {
            // triggered by State Notification from child resource

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/
            // HALT TCDS
            // HALT ferol/EVM/BU/RU?
            // HALT GEMFSMApplications

            return;
        }

        else if (obj instanceof StateEnteredEvent) {
            System.out.println("Executing haltAction");
            logger.info("Executing haltAction");

            // set action
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                                        new StringT("Requested to halt")));
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                                        new StringT("Halting")));

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            // halt TCDS
            if (functionManager.containerTCDSControllers != null) {
                if (!functionManager.containerTCDSControllers.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to halt TCDS on halt.");
                        functionManager.haltTCDSControllers();
                    } catch (UserActionException e) {
                        String msg = "[GEM FM::" + functionManager.FMname + "] haltAction: " + e.getMessage();
                        functionManager.goToError(msg,e);
                        // functionManager.sendCMSError(msg);  // when do this rather than goToError, or both?
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                        logger.error(msg);
                    }
                }
            }

            // halt GEMFSMApplications
            if (functionManager.containerGEMSupervisor != null) {
                if (!functionManager.containerGEMSupervisor.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to halt GEMSupervisor.");
                        functionManager.containerGEMSupervisor.execute(GEMInputs.HALT);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "[GEM FM::" + functionManager.FMname + "] haltAction: " + e.getMessage();
                        functionManager.goToError(msg,e);
                        // functionManager.sendCMSError(msg);  // when do this rather than goToError, or both?
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                        logger.error(msg);
                    }
                }
            }

            // ? ferol/EVM/BU/RU?
            if (functionManager.containerFEDKIT != null) {
                if (!functionManager.containerFEDKIT.isEmpty()) {
                    try {
                        logger.info("[GEM FM::" + functionManager.FMname + "] Trying to halt uFEDKIT resources on halt.");
                        functionManager.containerFEDKIT.execute(GEMInputs.HALT);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "[GEM FM::" + functionManager.FMname + "] haltAction: " + e.getMessage();
                        functionManager.goToError(msg,e);
                        // functionManager.sendCMSError(msg);  // when do this rather than goToError, or both?
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                        logger.error(msg);
                    }
                }
            }

            // check from which state we came.
            if (functionManager.getPreviousState().equals(GEMStates.TTSTEST_MODE)) {
                // when we came from TTSTestMode we need to
                // 1. give back control of sTTS to HW
            }


            // leave intermediate state
            functionManager.fireEvent( GEMInputs.SETHALTED );

            // Clean-up of the Function Manager parameters
            cleanUpFMParameters();

            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("")));
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,new StringT("Halted")));

            logger.debug("haltAction Executed");
        }
    }

    public void preparingTTSTestModeAction(Object obj)
        throws UserActionException
    {
        if (obj instanceof StateNotification) {
            // triggered by State Notification from child resource

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/
            // ? TCDS
            // ? ferol/EVM/BU/RU?
            // ? GEMFSMApplications

            return;
        }

        else if (obj instanceof StateEnteredEvent) {
            System.out.println("Executing preparingTestModeAction");
            logger.info("Executing preparingTestModeAction");

            // set action
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("preparingTestMode")));

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/
            // to prepare test we need to
            // 1. configure & enable fed application
            // 2. take control of fed

            // leave intermediate state
            functionManager.fireEvent( GEMInputs.SETTTSTEST_MODE );

            // set action
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("")));

            logger.debug("preparingTestModeAction Executed");
        }
    }

    public void testingTTSAction(Object obj)
        throws UserActionException
    {
        XdaqApplication fmm = null;
        Map attributeMap = new HashMap();

        if (obj instanceof StateNotification) {

            // triggered by State Notification from child resource

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/
            // ? TCDS
            // ? ferol/EVM/BU/RU?
            // ? GEMFSMApplications

            return;
        }

        else if (obj instanceof StateEnteredEvent) {
            System.out.println("Executing testingTTSAction");
            logger.info("Executing testingTTSAction");

            // set action
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("testing TTS")));

            // get the parameters of the command
            ParameterSet<CommandParameter> parameterSet = getUserFunctionManager().getLastInput().getParameterSet();

            // check parameter set
            if (parameterSet.size()==0 || parameterSet.get(GEMParameters.TTS_TEST_FED_ID) == null ||
                parameterSet.get(GEMParameters.TTS_TEST_MODE) == null ||
                ((StringT)parameterSet.get(GEMParameters.TTS_TEST_MODE).getValue()).equals("") ||
                parameterSet.get(GEMParameters.TTS_TEST_PATTERN) == null ||
                ((StringT)parameterSet.get(GEMParameters.TTS_TEST_PATTERN).getValue()).equals("") ||
                parameterSet.get(GEMParameters.TTS_TEST_SEQUENCE_REPEAT) == null)
                {

                    // go to error, we require parameters
                    String errMsg = "testingTTSAction: no parameters given with TestTTS command.";

                    // log error
                    logger.error(errMsg);

                    // notify error
                    sendCMSError(errMsg);

                    //go to error state
                    functionManager.fireEvent( GEMInputs.SETERROR );

                }

            Integer fedId  = ((IntegerT)parameterSet.get(GEMParameters.TTS_TEST_FED_ID).getValue()).getInteger();
            String mode    = ((StringT)parameterSet.get(GEMParameters.TTS_TEST_MODE).getValue()).getString();
            String pattern = ((StringT)parameterSet.get(GEMParameters.TTS_TEST_PATTERN).getValue()).getString();
            Integer cycles = ((IntegerT)parameterSet.get(GEMParameters.TTS_TEST_SEQUENCE_REPEAT).getValue()).getInteger();



            // debug
            logger.debug("Using parameters: fedId=" + fedId + "mode=" + mode + " pattern=" + pattern + " cycles=" + cycles );

            // find out which application controls the fedId.


            // found the correct application
            // to test we need to
            // 1. issue the test command


            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/


            // leave intermediate state
            functionManager.fireEvent( GEMInputs.SETTTSTEST_MODE );

            // set action
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("")));

            logger.debug("preparingTestModeAction Executed");
        }
    }

    public void coldResettingAction(Object obj)
        throws UserActionException
    {
        if (obj instanceof StateNotification) {
            // triggered by State Notification from child resource

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            logger.info("[GEM FM::" + functionManager.FMname + "] Recieved ColdResetting state notification");

            return;
        } else if (obj instanceof StateEnteredEvent) {
            System.out.println("Executing coldResettingAction");
            logger.info("Executing coldResettingAction");

            // set action
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                                        new StringT("coldResetting")));

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/
            // perform a cold-reset of your hardware

            // ? TCDS
            // ? ferol/EVM/BU/RU?
            // ? GEMFSMApplications

            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                                        new StringT("Cold Reset completed.")));
            // leave intermediate state
            functionManager.fireEvent( GEMInputs.SETHALTED );

            logger.debug("coldResettingAction Executed");
        }
    }

    public void fixSoftErrorAction(Object obj)
        throws UserActionException
    {
        if (obj instanceof StateNotification) {
            // triggered by State Notification from child resource

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            logger.info("[GEM FM::" + functionManager.FMname + "] Recieved FixSoftError state notification");

            return;
        } else if (obj instanceof StateEnteredEvent) {
            System.out.println("Executing fixSoftErrorAction");
            logger.info("Executing fixSoftErrorAction");

            // set action
            functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("fixingSoftError")));

            // get the parameters of the command
            ParameterSet<CommandParameter> parameterSet = getUserFunctionManager().getLastInput().getParameterSet();

            // check parameter set
            Long triggerNumberAtPause = null;
            if (parameterSet.size()==0 || parameterSet.get(GEMParameters.TRIGGER_NUMBER_AT_PAUSE) == null) {

                // go to error, we require parameters
                String warnMsg = "fixSoftErrorAction: no parameters given with fixSoftError command.";

                // log error
                logger.warn(warnMsg);

            } else {
                triggerNumberAtPause = ((LongT)parameterSet.get(GEMParameters.TRIGGER_NUMBER_AT_PAUSE).getValue()).getLong();
            }

            /************************************************
             * PUT YOUR CODE HERE TO FIX THE SOFT ERROR
             ***********************************************/

            // ? TCDS
            // ? ferol/EVM/BU/RU?
            // ? GEMFSMApplications

            functionManager.setSoftErrorDetected(false);


            // if the soft error cannot be fixed, the FM should go to ERROR

            if (functionManager.hasSoftError())
                functionManager.fireEvent(  GEMInputs.SETERROR  );
            else
                functionManager.fireEvent(  functionManager.isDegraded() ? GEMInputs.SETRUNNINGDEGRADED : GEMInputs.SETRUNNING  );

            // Clean-up of the Function Manager parameters
            cleanUpFMParameters();

            logger.debug("resumeAction Executed");
        }
    }

    //
    // for testing with external inputs.
    //
    // Here we just set our DEGRADED/SOFTERROR state according to an external trigger that sent us to this state.
    // In a real FM, an external event or periodic check will trigger the FM to change state.
    //
    //
    public void runningDegradedAction(Object obj)
        throws UserActionException
    {
        if (obj instanceof StateEnteredEvent) {
            functionManager.setDegraded(true);
            // ? TCDS
            // ? ferol/EVM/BU/RU?
            // ? GEMFSMApplications
        }
    }

    //
    // for testing with external inputs
    //
    // Here we just set our DEGRADED/SOFTERROR state according to an external trigger that sent us to this state.
    // In a real FM, an external event or periodic check will trigger the FM to change state.
    //
    //
    public void runningSoftErrorDetectedAction(Object obj)
        throws UserActionException
    {
        if (obj instanceof StateEnteredEvent) {
            logger.info("[GEM FM::" + functionManager.FMname + "] Recieved RunningSoftErrorDetected state notification");

            // do not touch degraded
            functionManager.setSoftErrorDetected(true);
            // ? TCDS
            // ? ferol/EVM/BU/RU?
            // ? GEMFSMApplications
        }
    }

    //
    // for testing with external inputs
    //
    // Here we just set our DEGRADED/SOFTERROR state according to an external trigger that sent us to this state.
    // In a real FM, an external event or periodic check will trigger the FM to change state.
    //
    //
    public void runningAction(Object obj)
        throws UserActionException
    {
        if (obj instanceof StateEnteredEvent) {
            logger.info("[GEM FM::" + functionManager.FMname + "] Recieved Running state notification");

            functionManager.setDegraded(false);
            functionManager.setSoftErrorDetected(false);
            // ? TCDS
            // ? ferol/EVM/BU/RU?
            // ? GEMFSMApplications
        }
    }


    @SuppressWarnings("unchecked")
	private void sendCMSError(String errMessage)
    {
        // create a new error notification msg
        CMSError error = functionManager.getErrorFactory().getCMSError();
        error.setDateTime(new Date().toString());
        error.setMessage(errMessage);

        // update error  parameter for GUI
        functionManager.getParameterSet().get(GEMParameters.ERROR_MSG).setValue(new StringT(errMessage));

        // send error
        try {
            functionManager.getParentErrorNotifier().sendError(error);
        } catch (Exception e) {
            logger.warn(functionManager.getClass().toString() + ": Failed to send error mesage " + errMessage);
        }
    }

    private void cleanUpFMParameters()
    {
        // Clean-up of the Function Manager parameters
        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("")));
        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ERROR_MSG,new StringT("")));
        functionManager.getParameterSet().put(new FunctionManagerParameter<IntegerT>(GEMParameters.TTS_TEST_FED_ID,new IntegerT(-1)));
    }

    protected void initXDAQ()
    {
        // Look if the configuration uses TCDS and handle accordingly.
        // First check if TCDS is being used, and if so, tell RCMS that the TCDS executives are already initialized.
        String msg = "[GEM FM::" + functionManager.FMname + "] Initializing XDAQ applications...";
        logger.info(msg);
        Boolean usingTCDS = false;
        QualifiedGroup qg = functionManager.getQualifiedGroup();

        List<QualifiedResource> xdaqExecutiveList = qg.seekQualifiedResourcesOfType(new XdaqExecutive());
        functionManager.containerXdaqExecutive    = new XdaqApplicationContainer(xdaqExecutiveList);

        // Always set TCDS executive and xdaq apps to initialized and the Job control to Active false
        maskTCDSExecAndJC(qg);

        for (QualifiedResource qr : xdaqExecutiveList) {
            String hostName = qr.getResource().getHostName();
            // ===WARNING!!!=== This hostname is hardcoded and should NOT be!!!
            // TODO This needs to be moved out into userXML or a snippet!!!
            if (hostName.equals("tcds-control-central.cms") ||
                hostName.equals("tcds-control-904.cms904") ) {
                usingTCDS = true;
                msg = "[GEM FM::" + functionManager.FMname + "] initXDAQ() -- the TCDS executive on hostName "
                    + hostName + " is being handled in a special way.";
                logger.info(msg);
                qr.setInitialized(true);
            }
        }

        List<QualifiedResource> jobControlList = qg.seekQualifiedResourcesOfType(new JobControl());
        functionManager.containerJobControl    = new XdaqApplicationContainer(jobControlList);

        for (QualifiedResource qr: jobControlList) {
            if (qr.getResource().getHostName().equals("tcds-control-central.cms") ||
                qr.getResource().getHostName().equals("tcds-control-904.cms904") ) {
                msg = "[GEM FM::" + functionManager.FMname + "] Masking the  application with name "
                    + qr.getName() + " running on host " + qr.getResource().getHostName();
                logger.info(msg);
                qr.setActive(false);
            }
        }

        // Start by getting XdaqServiceApplications
        List<QualifiedResource> xdaqServiceAppList      = qg.seekQualifiedResourcesOfType(new XdaqServiceApplication());
        functionManager.containerXdaqServiceApplication = new XdaqApplicationContainer(xdaqServiceAppList);

        List<String> applicationClasses = functionManager.containerXdaqServiceApplication.getApplicationClasses();
        for (String cla : applicationClasses) {
            msg = "[GEM FM::" + functionManager.FMname + "] found service application class: " + cla;
            logger.info(msg);
        }

        // TCDS apps -> Needs to be defined for GEM
        logger.info("[GEM FM::" + functionManager.FMname + "] Looking for TCDS");
        logger.info("[GEM FM::" + functionManager.FMname + "] Getting all LPMControllers");
        List<XdaqApplication> lpmList  = functionManager.containerXdaqServiceApplication.getApplicationsOfClass("tcds::lpm::LPMController");
        logger.info("[GEM FM::" + functionManager.FMname + "] Getting all ICIControllers");
        List<XdaqApplication> iciList  = functionManager.containerXdaqServiceApplication.getApplicationsOfClass("tcds::ici::ICIController");
        logger.info("[GEM FM::" + functionManager.FMname + "] Getting all PIControllers");
        List<XdaqApplication> piList   = functionManager.containerXdaqServiceApplication.getApplicationsOfClass("tcds::pi::PIController"  );
        logger.info("[GEM FM::" + functionManager.FMname + "] Making the TCDS list");
        List<XdaqApplication> tcdsList = new ArrayList<XdaqApplication>();

        logger.info("[GEM FM::" + functionManager.FMname + "] Adding TCDS applications to TCDS list");
        tcdsList.addAll(lpmList);
        tcdsList.addAll(iciList);
        tcdsList.addAll(piList);

        logger.info("[GEM FM::" + functionManager.FMname + "] Creating the TCDS containers");
        functionManager.containerTCDSControllers = new XdaqApplicationContainer(tcdsList);
        functionManager.containerLPMController   = new XdaqApplicationContainer(lpmList);
        functionManager.containerICIController   = new XdaqApplicationContainer(iciList);
        functionManager.containerPIController    = new XdaqApplicationContainer(piList);

        if ( qg.getRegistryEntry(GEMParameters.SID) == null) {
            Integer sid = ((IntegerT)functionManager.getParameterSet().get(GEMParameters.SID).getValue()).getInteger();
            qg.putRegistryEntry(GEMParameters.SID, sid);
            logger.warn("[GEM "+ functionManager.FMname + "] Just set the SID of QG to "+ sid);
        } else{
            logger.info("[GEM "+ functionManager.FMname + "] SID of QG is "+ qg.getRegistryEntry(GEMParameters.SID));
        }

        // Now if we are using TCDS, give all of the TCDS applications the URN that they need.
        try {
            msg = "[GEM FM::" + functionManager.FMname + "] initializing the qualified group";
            logger.info(msg);
            qg.init();
        } catch (Exception e) {
            // failed to init
            StringWriter sw = new StringWriter();
            e.printStackTrace( new PrintWriter(sw) );
            System.out.println(sw.toString());
            msg = "[GEM FM::" + functionManager.FMname + "] " + this.getClass().toString() +
                " failed to initialize resources. Printing stacktrace: "+ sw.toString();
            logger.error(msg);
            // functionManager.goToError(msg,e);
        }

        // find xdaq applications
        List<QualifiedResource> xdaqAppList        = qg.seekQualifiedResourcesOfType(new XdaqApplication());
        functionManager.containerXdaqApplication   = new XdaqApplicationContainer(xdaqAppList);

        applicationClasses = functionManager.containerXdaqApplication.getApplicationClasses();
        for (String cla : applicationClasses) {
            msg = "[GEM FM::" + functionManager.FMname + "] found application class: " + cla;
            logger.info(msg);
        }

        msg = "[GEM FM::" + functionManager.FMname + "] " + xdaqAppList.size() + " XDAQ applications controlled, " +
            xdaqServiceAppList.size() + " XDAQ service applications used";
        logger.debug(msg);

        // fill applications
        msg = "Retrieving GEM XDAQ applications ...";
        logger.info("[GEM FM::" + functionManager.FMname + "] "  + msg);
        functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG", new StringT(msg)));

        functionManager.containerGEMSupervisor = new XdaqApplicationContainer(functionManager.containerXdaqApplication.getApplicationsOfClass("gem::supervisor::GEMSupervisor"));
        if (!functionManager.containerGEMSupervisor.isEmpty()) {
            logger.info("[GEM FM::" + functionManager.FMname + "] GEM supervisor found! Welcome to GEMINI XDAQ control :)");
        } else {
            logger.info("[GEM FM::" + functionManager.FMname + "] GEM supervisor was not found!");
        }

        // Applications related to uFEDKIT readout
        List<XdaqApplication> fedKitList = new ArrayList<XdaqApplication>();

        // Ferol apps -> Needs to be defined for GEM
        logger.info("[GEM FM::" + functionManager.FMname + "] Looking for ferol");
        List<XdaqApplication> ferolList = functionManager.containerXdaqApplication.getApplicationsOfClass("ferol::FerolController");
        functionManager.containerFerol  = new XdaqApplicationContainer(ferolList);

        // evb apps -> Needs to be defined for GEM
        logger.info("[GEM FM::" + functionManager.FMname + "] Looking for evm");
        List<XdaqApplication> buList     = functionManager.containerXdaqApplication.getApplicationsOfClass("evb::BU");
        List<XdaqApplication> ruList     = functionManager.containerXdaqApplication.getApplicationsOfClass("evb::RU");
        List<XdaqApplication> evmList    = functionManager.containerXdaqApplication.getApplicationsOfClass("evb::EVM");

        functionManager.containerBU     = new XdaqApplicationContainer(buList);
        functionManager.containerRU     = new XdaqApplicationContainer(ruList);
        functionManager.containerEVM    = new XdaqApplicationContainer(evmList);


        fedKitList.addAll(ferolList);
        fedKitList.addAll(buList);
        fedKitList.addAll(ruList);
        fedKitList.addAll(evmList);
        functionManager.containerFEDKIT = new XdaqApplicationContainer(fedKitList);

        // find out if GEM supervisor is ready for async SOAP communication
        if (!functionManager.containerGEMSupervisor.isEmpty()) {
            logger.info("[GEM FM::" + functionManager.FMname + "] initXDAQ checking async SOAP communication with GEMSupervisor");

            XDAQParameter pam = null;

            String dowehaveanasyncgemSupervisor = "undefined";

            logger.info("[GEM FM::" + functionManager.FMname + "] initXDAQ: looping over qualified resources of GEMSupervisor type");
            // ask for the status of the GEM supervisor and wait until it is Ready or Failed
            for (QualifiedResource qr : functionManager.containerGEMSupervisor.getApplications() ){
                logger.info("[GEM FM::" + functionManager.FMname + "] initXDAQ: found qualified resource of GEMSupervisor type");
                try {
                    logger.info("[GEM FM::" + functionManager.FMname + "] initXDAQ: trying to get parameters");
                    pam =((XdaqApplication)qr).getXDAQParameter();
                    logger.info("[GEM FM::" + functionManager.FMname + "] initXDAQ: got parameters, selecting:");

                    pam.select(new String[] {"TriggerAdapterName", "PartitionState", "InitializationProgress","ReportStateToRCMS"});
                    logger.info("[GEM FM::" + functionManager.FMname + "] initXDAQ: parameters selected, getting:");
                    pam.get();
                    logger.info("[GEM FM::" + functionManager.FMname + "] initXDAQ: got selectedparameters!");

                    dowehaveanasyncgemSupervisor = pam.getValue("ReportStateToRCMS");
                    msg = "[GEM FM::" + functionManager.FMname + "] initXDAQ(): asking for the GEM supervisor "
                        + "ReportStateToRCMS results is: " + dowehaveanasyncgemSupervisor;
                    logger.info(msg);

                } catch (XDAQTimeoutException e) {
                    msg = "[GEM FM::" + functionManager.FMname + "] Error! XDAQTimeoutException in initXDAQ() when checking "
                        + "the async SOAP capabilities ...\n Perhaps the GEMSupervisor application is dead!?";
                    //functionManager.goToError(msg,e);
                    logger.error(msg);
                } catch (XDAQException e) {
                    msg = "[GEM FM::" + functionManager.FMname + "] Error! XDAQException in initXDAQ() when checking the async "
                        + "SOAP capabilities ...";
                    //functionManager.goToError(msg,e);
                    logger.error(msg);
                }

                logger.info("[GEM FM::" + functionManager.FMname + "] using async SOAP communication with GEMSupervisor ...");
            }
        } else {
            msg = "[GEM FM::" + functionManager.FMname + "] Warning! No GEM supervisor found in initXDAQ()."
                +"\nThis happened when checking the async SOAP capabilities.\nThis is OK for a level1 FM.";
            logger.info(msg);
        }
        /*
        // finally, halt all LPM apps
        functionManager.haltLPMControllers();

        // define the condition state vectors only here since the group must have been qualified before and all containers are filled
        functionManager.defineConditionState();
        functionManager.getHCALparameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT("")));
        */
    }

    void maskTCDSExecAndJC(QualifiedGroup qg)
    {
        // mark TCDS execs as initialized and mask their JobControl
        List<QualifiedResource> xdaqExecutiveList   = qg.seekQualifiedResourcesOfType(new XdaqExecutive());
        // why the duplicate
        List<QualifiedResource> xdaqServiceAppsList = qg.seekQualifiedResourcesOfType(new XdaqServiceApplication());
        // is this even needed?
        List<QualifiedResource> xdaqAppsList        = qg.seekQualifiedResourcesOfType(new XdaqApplication());

        //In case we turn TCDS to service app on-the-fly in the future...
        xdaqAppsList.addAll(xdaqServiceAppsList);

        // mask TCDS executive resources by hostname, role, or application name
        for (QualifiedResource qr : xdaqExecutiveList) {
            boolean foundTCDS = false;
            if (qr.getResource().getHostName().contains("tcds") ) {
                logger.info("[GEM FM::" + functionManager.FMname + "] TCDS found resource by hostname: "
                            + qr.getResource().getHostName());
                foundTCDS = true;
            } else if (qr.getResource().getRole().contains("tcds")) {
                logger.info("[GEM FM::" + functionManager.FMname + "] TCDS found resource by role: "
                            + qr.getResource().getRole());
                foundTCDS = true;
            } else if (qr.getResource().getName().contains("tcds")) {
                logger.info("[GEM FM::" + functionManager.FMname + "] TCDS found resource by name: "
                            + qr.getResource().getName());
                foundTCDS = true;
            }

            if (foundTCDS) {
                logger.info("[GEM FM::" + functionManager.FMname + "] found TCDS executive resource, masking resource and associated JC");
                qr.setInitialized(true);
                qg.seekQualifiedResourceOnPC(qr, new JobControl()).setActive(false);
            }
        }

        // mark TCDS apps as initialized by hostname, role or application name
        for (QualifiedResource qr : xdaqAppsList) {
            boolean foundTCDS = false;
            if (qr.getResource().getHostName().contains("tcds") ) {
                logger.info("[GEM FM::" + functionManager.FMname + "] TCDS found resource by hostname: "
                            + qr.getResource().getHostName());
                foundTCDS = true;
            } else if (qr.getResource().getRole().contains("tcds")) {
                logger.info("[GEM FM::" + functionManager.FMname + "] TCDS found resource by role: "
                            + qr.getResource().getRole());
                foundTCDS = true;
            } else if (qr.getResource().getName().contains("tcds")) {
                logger.info("[GEM FM::" + functionManager.FMname + "] TCDS found resource by name: "
                            + qr.getResource().getName());
                foundTCDS = true;
            }

            if (foundTCDS) {
                logger.info("[GEM FM::" + functionManager.FMname + "] found TCDS resource");
                qr.setInitialized(true);
            }
        }
    }
}
