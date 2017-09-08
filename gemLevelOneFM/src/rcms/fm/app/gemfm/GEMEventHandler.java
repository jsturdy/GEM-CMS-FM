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

    /**
     * <code>m_gemFM</code>: GEMFunctionManager
     */
    GEMFunctionManager m_gemFM = null;

    /**
     * <code>logger</code>: RCMS log4j logger
     */
    static RCMSLogger logger = new RCMSLogger(GEMEventHandler.class);

    /**
     * <code>m_gemQC</code>: QualifiedGroup of the GEM FM
     */
    private QualifiedGroup m_gemQG = null;

    /**
     * <code>m_gemPSet</code>: ParameterSet of the GEM FM
     */
    private ParameterSet<FunctionManagerParameter> m_gemPSet = null;

    /**
     * <code>SID</code>: Session ID for database connections
     */
    public Integer m_SID = 0;

    /**
     * <code>RunSequenceName</code>: Run sequence name, for attaining a run sequence number
     */
    public String RunSequenceName = "GEM test";

    /**
     * <code>RunSequenceNumber</code>: Run sequence number
     */
    public Integer m_RunSeqNumber = 0;

    public GEMEventHandler()
        throws rcms.fm.fw.EventHandlerException
    {
        // String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::GEMEventHandler(): ";
        String msgPrefix = "[GEM FM] GEMEventHandler::GEMEventHandler(): ";

        // this handler inherits UserStateNotificationHandler
        // so it is already registered for StateNotification events
        logger.info(msgPrefix + "Starting");

        // Let's register also the StateEnteredEvent triggered when the FSM enters in a new state.
        logger.info(msgPrefix + "Subscribing StateEnteredEvent events");
        subscribeForEvents(StateEnteredEvent.class);

        logger.info(msgPrefix + "Adding action callbacks");
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

        logger.info(msgPrefix + "Done");
    }


    public void init()
        throws rcms.fm.fw.EventHandlerException
    {
        String msgPrefix = "[GEM FM:: " + ((GEMFunctionManager)getUserFunctionManager()).m_FMname
            + "] GEMEventHandler::GEMEventHandler(): ";
        // String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::init(): ";

        logger.info(msgPrefix + "Starting");

        logger.info(msgPrefix + "Getting the user function manager");
        m_gemFM   = (GEMFunctionManager)getUserFunctionManager();
        logger.info(msgPrefix + "Getting the FM qualified group");
        m_gemQG   = m_gemFM.getQualifiedGroup();
        logger.info(msgPrefix + "Getting the FM parameter set");
        m_gemPSet = (ParameterSet<FunctionManagerParameter>)m_gemFM.getParameterSet();

        // debug
        logger.debug(msgPrefix + "Done");
    }


    // get official CMS run and sequence number
    protected RunNumberData getOfficialRunNumber()
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::getOfficialRunNumber(): ";

        // check availability of runInfo DB
        RunInfoConnectorIF ric = m_gemFM.getRunInfoConnector();
        // Get SID from parameter
        m_SID = ((IntegerT)m_gemPSet.get(GEMParameters.SID).getValue()).getInteger();
        if ( ric == null ) {
            logger.error(msgPrefix + "RunInfoConnector is empty. Is there a RunInfo DB or is it down?");

            // by default give run number 0
            return new RunNumberData(new Integer(m_SID),
                                     new Integer(0),m_gemFM.getOwner(),Calendar.getInstance().getTime());
        } else {
            RunSequenceNumber rsn = new RunSequenceNumber(ric,m_gemFM.getOwner(),RunSequenceName);
            RunNumberData     rnd = rsn.createRunSequenceNumber(m_SID);

            logger.info(msgPrefix + "Received run number: " + rnd.getRunNumber()
                        + " and sequence number: " + rnd.getSequenceNumber());

            m_gemFM.GEMRunInfo = null; // make RunInfo ready for the next round of run info to store
            return rnd;
        }
    }

    // establish connection to RunInfoDB - if needed
    protected void checkRunInfoDBConnection()
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::checkRunInfoDBConnection(): ";

        if (m_gemFM.GEMRunInfo == null) {
            logger.info(msgPrefix + "creating new RunInfo accessor with namespace: " + m_gemFM.GEM_NS);

            //Get SID from parameter
            m_SID = ((IntegerT)m_gemPSet.get(GEMParameters.SID).getValue()).getInteger();

            RunInfoConnectorIF ric = m_gemFM.getRunInfoConnector();

            m_gemFM.GEMRunInfo = new RunInfo(ric,m_SID,Integer.valueOf(m_gemFM.RunNumber));
            m_gemFM.GEMRunInfo.setNameSpace(m_gemFM.GEM_NS);

            logger.info(msgPrefix + "RunInfo accessor available.");
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

        public MoveTheGEMINI(Integer themaxmoves)
        {
            String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::MoveTheGEMINI(): ";

            movehimtotheright = true;
            moves = 0;
            offset = 0;
            maxmoves = themaxmoves;
            if (maxmoves < 30) { maxmoves = 30; }
            TheLine = "";
            theDice = new Random();
            logger.debug(msgPrefix + "The GEMINI should show up - Look at it as it moves through the sky");
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
                        } else if (saywhat == 5 && (maxmoves-moves) > 25) {
                            TheLine += " Swimming in dark matter!";
                            offset = 25;
                        } else if (saywhat == 6 && (maxmoves-moves) > 12) {
                            TheLine += " Where am I?";
                            offset = 12;
                        } else if (saywhat == 7 && (maxmoves-moves) > 20) {
                            TheLine += " Diving outer space";
                            offset = 20;
                        } else if (saywhat == 8 && (maxmoves-moves) > 18) {
                            TheLine += " I have two faces!";
                            offset = 18;
                        } else {
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
                } else {
                    Integer wheretogo = theDice.nextInt(10);
                    if (wheretogo >= 7) {
                        movehimtotheright = false;
                    }
                }
            } else {
                TheLine +="_";
                for (int count=moves; count > 1; count--) { TheLine +="_"; }
                TheLine += TheGEMINI;
                for (int count=maxmoves; count > moves; count--) { TheLine +="_"; }
                TheLine +="_";
                moves--;
                if (moves<1) {
                    movehimtotheright = true;
                    moves = 0;
                } else {
                    Integer wheretogo = theDice.nextInt(10);
                    if (wheretogo >= 7) {
                        movehimtotheright = true;
                    }
                }
            }
            m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(TheLine)));
        }
    }

    @SuppressWarnings("unchecked") // SHOULD REALLY MAKE SURE THAT THIS IS NECESSARY AND NOT JUST DUE TO BAD JAVA
        public void initAction(Object obj)
        throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::initAction(): ";

        if (obj instanceof StateNotification) {
            // triggered by State Notification from child resource

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            logger.info(msgPrefix + "Recieved Initialize state notification");

            return;
        } else if (obj instanceof StateEnteredEvent) {
            // triggered by entered state action
            // let's command the child resources

            // debug
            logger.debug(msgPrefix + "initAction called.");

            // set action
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG, new StringT("Initialize called")));
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,      new StringT("Initializing")));

            // get the parameters of the command
            Integer sid;
            String globalConfKey = null;

            try {
                ParameterSet<CommandParameter> inputParSet = m_gemFM.getLastInput().getParameterSet();
                // ParameterSet parameterSet = m_gemPSet;
                logger.info(msgPrefix + "input parameters " + inputParSet.getNames());
                if (inputParSet.get(GEMParameters.SID) != null) {
                    logger.info(msgPrefix + "found SID in lastInputParameterSet");
                    sid = ((IntegerT)inputParSet.get(GEMParameters.SID).getValue()).getInteger();
                    ((FunctionManagerParameter<IntegerT>)m_gemPSet
                     .get(GEMParameters.INITIALIZED_WITH_SID))
                        .setValue(new IntegerT(sid));
                    logger.info(msgPrefix + "INITIALIZED_WITH_SID has been set");
                    /*
                    // For the moment this parameter is only here to show if it is correctly set after initialization
                    // -> Really needed in future?
                    getParameterSet().get("INITIALIZED_WITH_SID").setValue(new IntegerT(sid));
                    */
                } else {
                    logger.warn(msgPrefix + "SID has been found to be null in lastInputParameterSet");
                }
                // globalConfKey = ((CommandParameter<StringT>)inputParSet.get(GEMParameters.GLOBAL_CONF_KEY)).getValue().toString();
            } catch (Exception e) {
                // go to error, we require parameters
                String msg = "error reading command parameters of Initialize command."
                    + e.getMessage();
                logger.error(msgPrefix + msg, e);
                // notify error
                sendCMSError(msg);
                // go to error state
                m_gemFM.fireEvent( GEMInputs.SETERROR );
                return;
            }

            // QualifiedGroup qg = m_gemFM.getQualifiedGroup();

            VectorT<StringT> availableResources = new VectorT<StringT>();

            List<QualifiedResource> qrList = m_gemQG.seekQualifiedResourcesOfType(new FunctionManager());
            for (QualifiedResource qr : qrList) {
                logger.info(msgPrefix + "function manager resource found: " + qr.getName());
                availableResources.add(new StringT(qr.getName()));
            }

            qrList = m_gemQG.seekQualifiedResourcesOfType(new XdaqExecutive());
            for (QualifiedResource qr : qrList) {
                logger.info(msgPrefix + "xdaq executive resource found: " + qr.getName());
                availableResources.add(new StringT(qr.getName()));
                // Snippet to get XDAQExecutive xml config
                XdaqExecutive exec = (XdaqExecutive)qr;
                XdaqExecutiveConfiguration config =  exec.getXdaqExecutiveConfiguration();
                String ExecXML = config.getXml();
                String EnvironmentLoaded = config.getEnvironmentString();
                // m_gemPSet.put(new FunctionManagerParameter<StringT>("XML_Executive",new StringT(ExecXML)));
                m_gemPSet.put(new FunctionManagerParameter<StringT>("Environment_settings",
                                                                    new StringT(EnvironmentLoaded)));
                logger.info("GEM FM::" + m_gemFM.m_FMname + "] Executive config "     + ExecXML);
                logger.info("GEM FM::" + m_gemFM.m_FMname + "] Environment settings " + EnvironmentLoaded);
            }

            // Looking for job control resources
            qrList = m_gemQG.seekQualifiedResourcesOfType(new JobControl());
            // logger.info(msgPrefix + "Looking for job control resources");
            for (QualifiedResource qr : qrList) {
                logger.info(msgPrefix + "job control resource found: " + qr.getName());
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

            qrList = m_gemQG.seekQualifiedResourcesOfType(new XdaqApplication());
            for (QualifiedResource qr : qrList) {
                logger.info(msgPrefix + "xdaq application resource found: " + qr.getName());
                if (qr.getName().contains("Manager") || qr.getName().contains("Readout")) {
                    // qr.setActive(false);  // shouldn't be necessary if GEMSupervisor correctly handles the initialize case
                    continue;
                }
                availableResources.add(new StringT(qr.getName()));
            }

            m_gemPSet.put(new FunctionManagerParameter<VectorT<StringT> >("AVAILABLE_RESOURCES",
                                                                          availableResources));

            // initialize all XDAQ executives
            try {
                logger.info(msgPrefix + "calling initXDAQ");
                initXDAQ();
            } catch (UserActionException e) {
                String msg = "initXDAQ failed ";
                m_gemFM.goToError(msg, e);
                // m_gemFM.sendCMSError(msg);  // when do this rather than goToError, or both?
                m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                logger.error(msgPrefix + msg, e);
            }

            logger.info(msgPrefix + "initXDAQ finished");

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            // force TCDS HALTED
            if (m_gemFM.c_tcdsControllers != null) {
                if (!m_gemFM.c_tcdsControllers.isEmpty()) {
                    try {
                        logger.info(msgPrefix + "Trying to halt TCDS on initialize.");
                        m_gemFM.haltTCDSControllers();
                    } catch (UserActionException e) {
                        String msg = "Caught exception";
                        m_gemFM.goToError(msg, e);
                        // m_gemFM.sendCMSError(msg);  // when do this rather than goToError, or both?
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                        logger.error(msgPrefix + msg, e);
                    }
                }
            }

            // initialize GEMFSMApplications
            if (m_gemFM.c_gemSupervisors != null) {
                if (!m_gemFM.c_gemSupervisors.isEmpty()) {
                    try {
                        logger.info(msgPrefix + "Trying to initialize GEMSupervisor.");
                        m_gemFM.c_gemSupervisors.execute(GEMInputs.INITIALIZE);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "Caught exception";
                        m_gemFM.goToError(msg, e);
                        // m_gemFM.sendCMSError(msg);  // when do this rather than goToError, or both?
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",     new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                        logger.error(msgPrefix + msg, e);
                    }
                }
            }

            // ? ferol/EVM/BU/RU?
            if (m_gemFM.c_uFEDKIT != null) {
                if (!m_gemFM.c_uFEDKIT.isEmpty()) {
                    try {
                        logger.info(msgPrefix + "Trying to halt uFEDKIT resources on initialize.");
                        m_gemFM.c_uFEDKIT.execute(GEMInputs.HALT);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "Caught QualifiedResourceContainerException";
                        m_gemFM.goToError(msg, e);
                        // m_gemFM.sendCMSError(msg);  // when do this rather than goToError, or both?
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",     new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                        logger.error(msgPrefix + msg, e);
                    }
                }
            }
            /*
            // set exported parameters
            ((FunctionManagerParameter<IntegerT>)m_gemPSet
            .get(GEMParameters.INITIALIZED_WITH_SID))
            .setValue(new IntegerT(m_SID));
            ((FunctionManagerParameter<StringT>)m_gemPSet
            .get(GEMParameters.INITIALIZED_WITH_GLOBAL_CONF_KEY))
            .setValue(new StringT(globalConfKey));
            */
            // go to HALT
            m_gemFM.fireEvent( GEMInputs.SETHALTED );

            // set action
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                new StringT("")));
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                new StringT("Initialized -> Halted")));

            logger.info("initAction Executed");
        }
    }


    public void resetAction(Object obj)
        throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::resetAction(): ";

        if (obj instanceof StateNotification) {
            // triggered by State Notification from child resource

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            logger.info(msgPrefix + "Recieved Reset state notification");

            return;
        } else if (obj instanceof StateEnteredEvent) {
            // triggered by entered state action
            // let's command the child resources

            // debug
            logger.debug("resetAction called.");

            // set action
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                new StringT("Reset called")));
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                new StringT("Resetting")));

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            m_gemFM.destroyXDAQ();

            // initialize all XDAQ executives
            try {
                logger.info(msgPrefix + "calling initXDAQ");
                initXDAQ();
            } catch (UserActionException e) {
                String msg = "initXDAQ failed ";
                m_gemFM.goToError(msg, e);
                // m_gemFM.sendCMSError(msg);  // when do this rather than goToError, or both?
                m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                logger.error(msgPrefix + msg, e);
            }

            // force TCDS HALTED
            if (m_gemFM.c_tcdsControllers != null) {
                if (!m_gemFM.c_tcdsControllers.isEmpty()) {
                    try {
                        logger.info(msgPrefix + "Trying to halt TCDS on reset.");
                        m_gemFM.haltTCDSControllers();
                    } catch (UserActionException e) {
                        String msg = "Caught UserActionException";
                        logger.warn(msgPrefix + msg, e);
                    }
                }
            }

            // reset GEMFSMApplications and then send INITIALIZE
            if (m_gemFM.c_gemSupervisors != null) {
                if (!m_gemFM.c_gemSupervisors.isEmpty()) {
                    try {
                        logger.info(msgPrefix + "Trying to reset GEMSupervisor");
                        m_gemFM.c_gemSupervisors.execute(GEMInputs.RESET);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "Caught QualifiedResourceContainerException";
                        logger.warn(msgPrefix + msg, e);
                    }

                    // need to now initialize GEM resoureces as RESET puts them into INITIAL state,
                    // but RCMS puts things into HALTED state

                    try {
                        logger.info(msgPrefix + "Trying to initialize GEMSupervisor");
                        m_gemFM.c_gemSupervisors.execute(GEMInputs.INITIALIZE);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "Caught QualifiedResourceContainerException";
                        logger.warn(msgPrefix + msg, e);
                    }
                }
            }

            // ? ferol/EVM/BU/RU?
            if (m_gemFM.c_uFEDKIT != null) {
                if (!m_gemFM.c_uFEDKIT.isEmpty()) {
                    try {
                        logger.info(msgPrefix + "Trying to halt uFEDKIT resources on reset.");
                        m_gemFM.c_uFEDKIT.execute(GEMInputs.HALT);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "Caught QualifiedResourceContainerException";
                        logger.warn(msgPrefix + msg, e);
                    }
                }
            }

            m_gemFM.GEMRunInfo = null; // make RunInfo ready for the next round of run info to store

            // go to Initital
            m_gemFM.fireEvent( GEMInputs.SETHALTED );

            // Clean-up of the Function Manager parameters
            cleanUpFMParameters();

            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                new StringT("")));
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                new StringT("Reset - Halted")));

            logger.info("resetAction Executed");
        }
    }

    public void recoverAction(Object obj)
        throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::recoverAction(): ";

        if (obj instanceof StateNotification) {
            // triggered by State Notification from child resource

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            logger.info(msgPrefix + "Recieved Recover state notification");

            return;
        } else if (obj instanceof StateEnteredEvent) {

            System.out.println("Executing recoverAction");
            logger.info("Executing recoverAction");

            // set action
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("recovering")));

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/
            // force TCDS HALTED
            // force GEMFSMApplications HALTED
            // force ferol/EVM/BU/RU HALTED?

            // leave intermediate state
            m_gemFM.fireEvent( GEMInputs.SETHALTED );

            // Clean-up of the Function Manager parameters
            cleanUpFMParameters();

            logger.info("recoverAction Executed");
        }
    }

    @SuppressWarnings("unchecked") // SHOULD REALLY MAKE SURE THAT THIS IS NECESSARY AND NOT JUST DUE TO BAD JAVA
        public void configureAction(Object obj)
        throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::configureAction(): ";

        if (obj instanceof StateNotification) {
            // triggered by State Notification from child resource

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            logger.info(msgPrefix + "Recieved Configure state notification");

            return;
        } else if (obj instanceof StateEnteredEvent) {
            System.out.println(msgPrefix + "Executing configureAction");
            logger.info(msgPrefix + "Executing configureAction");

            // set action
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                new StringT("Configure action called")));
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                new StringT("Configuring")));

            // get the parameters of the command
            Integer runNumber     = -1;
            String  runKey        = "N/A";
            String  fedEnableMask = "N/A";

            try {
                ParameterSet<CommandParameter> inputParSet = m_gemFM.getLastInput().getParameterSet();
                if (inputParSet.size() == 0)  {
                    logger.info(msgPrefix + "inputParSet is empty");

                    // if ( m_gemQG.getRegistryEntry(GEMParameters.RUN_KEY) == null) {
                    //     runKey = ((StringT)m_gemPSet.get(GEMParameters.RUN_KEY).getValue()).getString();
                    //     m_gemQG.putRegistryEntry(GEMParameters.RUN_KEY, runKey.toString());
                    //     logger.info("[GEM "+ m_gemFM.m_FMname + "] Just set the RUN_KEY of QG to " + runKey);
                    // } else {
                    //     logger.info("[GEM "+ m_gemFM.m_FMname + "] RUN_KEY of QG is "
                    //                 + m_gemQG.getRegistryEntry(GEMParameters.RUN_KEY));
                    // }

                    // if ( m_gemQG.getRegistryEntry(GEMParameters.RUN_NUMBER) == null) {
                    //     runNumber = ((IntegerT)m_gemPSet.get(GEMParameters.RUN_NUMBER).getValue()).getInteger();
                    //     m_gemQG.putRegistryEntry(GEMParameters.RUN_NUMBER, runNumber.toString());
                    //     logger.info("[GEM "+ m_gemFM.m_FMname + "] Just set the RUN_NUMBER of QG to " + runNumber);
                    // } else {
                    //     logger.info("[GEM "+ m_gemFM.m_FMname + "] RUN_NUMBER of QG is "
                    //                 + m_gemQG.getRegistryEntry(GEMParameters.RUN_NUMBER));
                    // }

                    // if ( m_gemQG.getRegistryEntry(GEMParameters.FED_ENABLE_MASK) == null) {
                    //     fedEnableMask = ((StringT)m_gemPSet.get(GEMParameters.FED_ENABLE_MASK).getValue()).getString();
                    //     m_gemQG.putRegistryEntry(GEMParameters.FED_ENABLE_MASK, fedEnableMask.toString());
                    //     logger.info("[GEM "+ m_gemFM.m_FMname + "] Just set the FED_ENABLE_MASK of QG to " + fedEnableMask);
                    // } else {
                    //     logger.info("[GEM "+ m_gemFM.m_FMname + "] FED_ENABLE_MASK of QG is "
                    //                 + m_gemQG.getRegistryEntry(GEMParameters.FED_ENABLE_MASK));
                    // }
                } else {
                    runNumber     = ((CommandParameter<IntegerT>)inputParSet.get(GEMParameters.RUN_NUMBER)).getValue().getInteger();
                    runKey        = ((CommandParameter<StringT>)inputParSet.get(GEMParameters.RUN_KEY)).getValue().toString();
                    fedEnableMask = ((CommandParameter<StringT>)inputParSet.get(GEMParameters.FED_ENABLE_MASK)).getValue().toString();

                    /*
                      runNumber     = ((IntegerT)inputParSet.get(GEMParameters.RUN_NUMBER).getValue()).getInteger();
                      runKey        = ((StringT)inputParSet.get(GEMParameters.RUN_KEY).getValue()).toString();
                      fedEnableMask = ((StringT)inputParSet.get(GEMParameters.FED_ENABLE_MASK).getValue()).toString();
                    */
                }
            } catch (Exception e) {
                // go to error, we require parameters
                String msg = "error reading command parameters of Configure command.";
                logger.error(msgPrefix + msg, e);
                sendCMSError(msg);
                //go to error state
                m_gemFM.fireEvent( GEMInputs.SETERROR );
                return;
            }

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            // Set the configuration parameters in the Function Manager parameters
            ((FunctionManagerParameter<IntegerT>)m_gemPSet
             .get(GEMParameters.CONFIGURED_WITH_RUN_NUMBER))
                .setValue(new IntegerT(runNumber));
            ((FunctionManagerParameter<StringT>)m_gemPSet
             .get(GEMParameters.CONFIGURED_WITH_RUN_KEY))
                .setValue(new StringT(runKey));
            ((FunctionManagerParameter<StringT>)m_gemPSet
             .get(GEMParameters.CONFIGURED_WITH_FED_ENABLE_MASK))
                .setValue(new StringT(fedEnableMask));

            // configure TCDS (LPM then ICI then PI)
            if (m_gemFM.c_tcdsControllers != null) {
                if (!m_gemFM.c_tcdsControllers.isEmpty()) {
                    try {
                        logger.info(msgPrefix + "Trying to configure TCDS on configure.");
                        m_gemFM.configureTCDSControllers();
                    } catch (UserActionException e) {
                        String msg = "Caught UserActionException";
                        logger.error(msgPrefix + msg, e);
                        //m_gemFM.sendCMSError(msg);
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                    }
                }
            }

            // configure GEMFSMApplications
            if (m_gemFM.c_gemSupervisors != null) {
                if (!m_gemFM.c_gemSupervisors.isEmpty()) {
                    try {
                        logger.info(msgPrefix + "Trying to configure GEMSupervisor.");
                        m_gemFM.c_gemSupervisors.execute(GEMInputs.CONFIGURE);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "Caught QualifiedResourceContainerException";
                        logger.error(msgPrefix + msg, e);
                        //m_gemFM.sendCMSError(msg);
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                    }
                }
            }

            // configure ferol/EVM/BU/RU?
            // need to send the FED_ENABLE_MASK to the EVM and BU
            // need to configure first the EVM then BU and then the FerolController
            /*
              if (m_gemFM.c_uFEDKIT != null) {
              if (!m_gemFM.c_uFEDKIT.isEmpty()) {
              try {
              logger.info(msgPrefix + "Trying to configure uFEDKIT resources on configure.");
              m_gemFM.c_uFEDKIT.execute(GEMInputs.CONFIGURE);
              } catch (QualifiedResourceContainerException e) {
              String msg = "Caught QualifiedResourceContainerException";
              logger.error(msgPrefix + msg, e);
              //m_gemFM.sendCMSError(msg);
              m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
              m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
              }
              }
              }
            */
            // need to ensure that necessary paramters are sent
            // these applications expect empty command transitions it seems
            if (m_gemFM.c_EVMs != null) {
                if (!m_gemFM.c_EVMs.isEmpty()) {
                    try {
                        logger.info(msgPrefix + "Trying to configure EVM resources on configure.");
                        m_gemFM.c_EVMs.execute(GEMInputs.CONFIGURE);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "Caught QualifiedResourceContainerException";
                        logger.error(msgPrefix + msg, e);
                        //m_gemFM.sendCMSError(msg);
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                    }
                }
            }

            if (m_gemFM.c_BUs != null) {
                if (!m_gemFM.c_BUs.isEmpty()) {
                    try {
                        logger.info(msgPrefix + "Trying to configure BU resources on configure.");
                        m_gemFM.c_BUs.execute(GEMInputs.CONFIGURE);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "Caught QualifiedResourceContainerException";
                        logger.error(msgPrefix + msg, e);
                        //m_gemFM.sendCMSError(msg);
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                    }
                }
            }

            if (m_gemFM.c_Ferols != null) {
                if (!m_gemFM.c_Ferols.isEmpty()) {
                    try {
                        logger.info(msgPrefix + "Trying to configure Ferol resources on configure.");
                        m_gemFM.c_Ferols.execute(GEMInputs.CONFIGURE);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "Caught QualifiedResourceContainerException";
                        logger.error(msgPrefix + msg, e);
                        //m_gemFM.sendCMSError(msg);
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                    }
                }
            }

            // leave intermediate state
            m_gemFM.fireEvent( GEMInputs.SETCONFIGURED );

            // set action
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("")));
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,new StringT("Configured")));

            logger.info("configureAction Executed");
        }
    }

    public void startAction(Object obj)
        throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::startAction(): ";

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
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                new StringT("Started action called!")));
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                new StringT("Starting!!!")));

            // get the parameters of the command
            ParameterSet<CommandParameter> inputParSet = m_gemFM.getLastInput().getParameterSet();

            if (m_gemFM.getRunInfoConnector() != null) {
                RunNumberData rnd = getOfficialRunNumber();

                m_gemFM.RunNumber = rnd.getRunNumber();
                m_RunSeqNumber    = rnd.getSequenceNumber();

                m_gemPSet.put(new FunctionManagerParameter<IntegerT>("RUN_NUMBER",
                                                                     new IntegerT(m_gemFM.RunNumber)));
                m_gemPSet.put(new FunctionManagerParameter<IntegerT>("RUN_SEQ_NUMBER",
                                                                     new IntegerT(m_RunSeqNumber)));
                logger.info(msgPrefix + "run number: " + m_gemFM.RunNumber
                            + ", SequenceNumber: " + m_RunSeqNumber);
            } else {
                logger.error(msgPrefix + "Official RunNumber requested, but cannot establish "
                             +  "RunInfo Connection. Is there a RunInfo DB? or is RunInfo DB down?");
                logger.info(msgPrefix + "Going to use run number =" + m_gemFM.RunNumber
                            +  ", RunSeqNumber = " +  m_RunSeqNumber);
            }

            // check parameter set
            /*if (inputParSet.size()==0 || inputParSet.get(GEMParameters.RUN_NUMBER) == null )  {

            // go to error, we require parameters
            String errMsg = "no parameters given with start command.";

            // log error
            logger.error(msgPrefix + errMsg, e);

            // notify error
            sendCMSError(errMsg);

            // go to error state
            m_gemFM.fireEvent( GEMInputs.SETERROR );
            return;
            }*/

            logger.info(msgPrefix + "getting the run number");

            // get the run number from the start command, where does it come from?
            // Integer runNumber = ((IntegerT)inputParSet.get(GEMParameters.RUN_NUMBER).getValue()).getInteger();
            Integer runNumber = m_gemFM.RunNumber;

            logger.info(msgPrefix + "updating the the started-with run number");
            m_gemPSet.put(new FunctionManagerParameter<IntegerT>(GEMParameters.STARTED_WITH_RUN_NUMBER,
                                                                 new IntegerT(runNumber)));

            // Set the run number in the Function Manager parameters
            logger.info(msgPrefix + "updating the run number");
            m_gemPSet.put(new FunctionManagerParameter<IntegerT>(GEMParameters.RUN_NUMBER,
                                                                 new IntegerT(runNumber)));

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/
            /*
            // Set the run number in the Function Manager parameters
            ((FunctionManagerParameter<IntegerT>)m_gemPSet
            .get(GEMParameters.STARTED_WITH_RUN_NUMBER))
            .setValue(new IntegerT(runNumber));
            */

            logger.info(msgPrefix + "runNumber is " + runNumber);

            // START GEMFSMApplications
            if (m_gemFM.c_gemSupervisors != null) {
                if (!m_gemFM.c_gemSupervisors.isEmpty()) {
                    XDAQParameter pam = null;
                    // prepare and set for all GEM supervisors the RunType
                    for (QualifiedResource qr : m_gemFM.c_gemSupervisors.getApplications() ){
                        try {
                            pam = ((XdaqApplication)qr).getXDAQParameter();
                            pam.select(new String[] {"RunNumber"});
                            pam.setValue("RunNumber",m_gemFM.RunNumber.toString());
                            logger.info(msgPrefix + "sending run number to the supervisor");
                            pam.send();
                            logger.info(msgPrefix + "sent run number to the supervisor");
                        } catch (XDAQTimeoutException e) {
                            String msg = "Error! XDAQTimeoutException when "
                                + " trying to send the m_gemFM.RunNumber to the GEM supervisor\n Perhaps this "
                                + "application is dead!?";
                            m_gemFM.goToError(msg, e);
                            logger.error(msgPrefix + msg);
                        } catch (XDAQException e) {
                            String msg = "Error! XDAQException when trying "
                                + "to send the m_gemFM.RunNumber to the GEM supervisor";
                            m_gemFM.goToError(msg, e);
                            logger.error(msgPrefix + msg);
                        }
                    }

                    try {
                        logger.info(msgPrefix + "Trying to start GEMSupervisor.");
                        m_gemFM.c_gemSupervisors.execute(GEMInputs.START);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "Caught QualifiedResourceContainerException";
                        logger.error(msgPrefix + msg, e);
                        //m_gemFM.sendCMSError(msg);
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                    }
                }
            }

            // ENABLE? ferol/EVM/BU/RU?
            // need to start first the EVM then BU and then the FerolController
            /*
              if (m_gemFM.c_uFEDKIT != null) {
              if (!m_gemFM.c_uFEDKIT.isEmpty()) {
              try {
              logger.info(msgPrefix + "Trying to enable uFEDKIT resources on start.");
              m_gemFM.c_uFEDKIT.execute(GEMInputs.ENABLE);
              } catch (QualifiedResourceContainerException e) {
              String msg = "Caught QualifiedResourceContainerException";
              logger.error(msgPrefix + msg, e);
              m_gemFM.goToError(msg, e);
              // m_gemFM.sendCMSError(msg);  // when do this rather than goToError, or both?
              m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
              m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
              }
              }
              }
            */

            if (m_gemFM.c_EVMs != null) {
                if (!m_gemFM.c_EVMs.isEmpty()) {
                    XDAQParameter pam = null;
                    // prepare and set the runNumber for all EVMs
                    for (QualifiedResource qr : m_gemFM.c_EVMs.getApplications() ){
                        try {
                            pam = ((XdaqApplication)qr).getXDAQParameter();
                            pam.select(new String[] {"runNumber"});
                            pam.get();
                            String evmRunNumber = pam.getValue("RunNumber");
                            logger.info(msgPrefix + "Obtained run number from evm: " + evmRunNumber);
                            pam.setValue("runNumber",m_gemFM.RunNumber.toString());
                            pam.send();
                        } catch (XDAQTimeoutException e) {
                            String msg = "Error! XDAQTimeoutException: startAction() when "
                                + " trying to send the m_gemFM.RunNumber to the GEM supervisor\n Perhaps this "
                                + "application is dead!?";
                            m_gemFM.goToError(msg, e);
                            logger.error(msgPrefix + msg);
                        } catch (XDAQException e) {
                            String msg = "Error! XDAQException: startAction() when trying "
                                + "to send the m_gemFM.RunNumber to the GEM supervisor";
                            m_gemFM.goToError(msg, e);
                            logger.error(msgPrefix + msg);
                        }
                    }

                    try {
                        logger.info(msgPrefix + "Trying to enable EVM resources on start.");
                        m_gemFM.c_EVMs.execute(GEMInputs.ENABLE);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "Caught QualifiedResourceContainerException";
                        logger.error(msgPrefix + msg, e);
                        //m_gemFM.sendCMSError(msg);
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                    }
                }
            }

            if (m_gemFM.c_BUs != null) {
                if (!m_gemFM.c_BUs.isEmpty()) {
                    XDAQParameter pam = null;
                    // prepare and set the runNumber for all BUs
                    for (QualifiedResource qr : m_gemFM.c_BUs.getApplications() ){
                        try {
                            pam = ((XdaqApplication)qr).getXDAQParameter();
                            pam.select(new String[] {"runNumber"});
                            pam.get();
                            String buRunNumber = pam.getValue("RunNumber");
                            logger.info(msgPrefix + "Obtained run number from bu: " + buRunNumber);
                            pam.setValue("runNumber",m_gemFM.RunNumber.toString());
                            pam.send();
                        } catch (XDAQTimeoutException e) {
                            String msg = "Error! XDAQTimeoutException: startAction() when "
                                + " trying to send the m_gemFM.RunNumber to the GEM supervisor\n Perhaps this "
                                + "application is dead!?";
                            m_gemFM.goToError(msg, e);
                            logger.error(msgPrefix + msg);
                        } catch (XDAQException e) {
                            String msg = "Error! XDAQException: startAction() when trying "
                                + "to send the m_gemFM.RunNumber to the GEM supervisor";
                            m_gemFM.goToError(msg, e);
                            logger.error(msgPrefix + msg);
                        }
                    }

                    try {
                        logger.info(msgPrefix + "Trying to enable BU resources on start.");
                        m_gemFM.c_BUs.execute(GEMInputs.ENABLE);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "Caught QualifiedResourceContainerException";
                        logger.error(msgPrefix + msg, e);
                        //m_gemFM.sendCMSError(msg);
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                    }
                }
            }

            if (m_gemFM.c_Ferols != null) {
                if (!m_gemFM.c_Ferols.isEmpty()) {
                    try {
                        logger.info(msgPrefix + "Trying to enable Ferol resources on start.");
                        m_gemFM.c_Ferols.execute(GEMInputs.ENABLE);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "Caught QualifiedResourceContainerException";
                        logger.error(msgPrefix + msg, e);
                        //m_gemFM.sendCMSError(msg);
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                    }
                }
            }

            // ENABLE TCDS
            if (m_gemFM.c_tcdsControllers != null) {
                if (!m_gemFM.c_tcdsControllers.isEmpty()) {
                    try {
                        logger.info(msgPrefix + "Trying to enable TCDS resources on start.");
                        m_gemFM.enableTCDSControllers();
                    } catch (UserActionException e) {
                        String msg = "Caught UserActionException";
                        logger.error(msgPrefix + msg, e);
                        m_gemFM.goToError(msg, e);
                        // m_gemFM.sendCMSError(msg);  // when do this rather than goToError, or both?
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                    }
                }
            }

            // leave intermediate state
            m_gemFM.fireEvent( GEMInputs.SETRUNNING );

            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                new StringT("")));
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                new StringT("Running!!!")));
            logger.debug("startAction Executed");
        }
    }

    public void pauseAction(Object obj)
        throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::pauseAction(): ";

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
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                new StringT("Pause action issued")));
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                new StringT("Pausing")));

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            // pause TCDS
            if (m_gemFM.c_tcdsControllers != null) {
                if (!m_gemFM.c_tcdsControllers.isEmpty()) {
                    try {
                        logger.info(msgPrefix + "Trying to pause TCDS on pause.");
                        m_gemFM.pauseTCDSControllers();
                    } catch (UserActionException e) {
                        String msg = "Caught UserActionException";
                        m_gemFM.goToError(msg, e);
                        // m_gemFM.sendCMSError(msg);  // when do this rather than goToError, or both?
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                        logger.error(msgPrefix + msg, e);
                    }
                }
            }

            // pause GEMFSMApplications
            if (m_gemFM.c_gemSupervisors != null) {
                if (!m_gemFM.c_gemSupervisors.isEmpty()) {
                    try {
                        logger.info(msgPrefix + "Trying to pause GEMSupervisor.");
                        m_gemFM.c_gemSupervisors.execute(GEMInputs.PAUSE);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "Caught QualifiedResourceContainerException";
                        m_gemFM.goToError(msg, e);
                        // m_gemFM.sendCMSError(msg);  // when do this rather than goToError, or both?
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                        logger.error(msgPrefix + msg, e);
                    }
                }
            }

            /*
            // PAUSE ferol/EVM/BU/RU?
            if (m_gemFM.c_uFEDKIT != null) {
            if (!m_gemFM.c_uFEDKIT.isEmpty()) {
            try {
            logger.info(msgPrefix + "Trying to pause uFEDKIT resources on pause.");
            m_gemFM.c_uFEDKIT.execute(GEMInputs.PAUSE);
            } catch (QualifiedResourceContainerException e) {
            String msg = "Caught QualifiedResourceContainerException";
            m_gemFM.goToError(msg, e);
            // m_gemFM.sendCMSError(msg);  // when do this rather than goToError, or both?
            m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
            m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
            logger.error(msgPrefix + msg, e);
            }
            }
            }
            */

            // leave intermediate state
            m_gemFM.fireEvent( GEMInputs.SETPAUSED );

            // set action
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("")));
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,new StringT("Paused")));

            logger.debug("pausingAction Executed");
        }
    }

    public void stopAction(Object obj)
        throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::stopAction(): ";

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
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                new StringT("Stop requested")));
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                new StringT("Stopping")));

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            // stop TCDS
            if (m_gemFM.c_tcdsControllers != null) {
                if (!m_gemFM.c_tcdsControllers.isEmpty()) {
                    try {
                        logger.info(msgPrefix + "Trying to stop TCDS on stop.");
                        m_gemFM.stopTCDSControllers();
                    } catch (UserActionException e) {
                        String msg = "Caught UserActionException";
                        m_gemFM.goToError(msg, e);
                        // m_gemFM.sendCMSError(msg);  // when do this rather than goToError, or both?
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                        logger.error(msgPrefix + msg, e);
                    }
                }
            }

            // stop GEMFSMApplications
            if (m_gemFM.c_gemSupervisors != null) {
                if (!m_gemFM.c_gemSupervisors.isEmpty()) {
                    try {
                        logger.info(msgPrefix + "Trying to stop GEMSupervisor.");
                        m_gemFM.c_gemSupervisors.execute(GEMInputs.STOP);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "Caught QualifiedResourceContainerException";
                        m_gemFM.goToError(msg, e);
                        // m_gemFM.sendCMSError(msg);  // when do this rather than goToError, or both?
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                        logger.error(msgPrefix + msg, e);
                    }
                }
            }

            // ? ferol/EVM/BU/RU?
            // stop ferol then BU then EVM
            /*
              if (m_gemFM.c_uFEDKIT != null) {
              if (!m_gemFM.c_uFEDKIT.isEmpty()) {
              try {
              logger.info(msgPrefix + "Trying to stop uFEDKIT resources on stop.");
              m_gemFM.c_uFEDKIT.execute(GEMInputs.STOP);
              } catch (QualifiedResourceContainerException e) {
              String msg = "Caught QualifiedResourceContainerException";
              m_gemFM.goToError(msg, e);
              // m_gemFM.sendCMSError(msg);  // when do this rather than goToError, or both?
              m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
              m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
              logger.error(msgPrefix + msg, e);
              }
              }
              }
            */

            if (m_gemFM.c_EVMs != null) {
                if (!m_gemFM.c_EVMs.isEmpty()) {
                    try {
                        logger.info(msgPrefix + "Trying to stop EVM resources on stop.");
                        m_gemFM.c_EVMs.execute(GEMInputs.STOP);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "Caught QualifiedResourceContainerException";
                        logger.error(msgPrefix + msg, e);
                        //m_gemFM.sendCMSError(msg);
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                    }
                }
            }

            if (m_gemFM.c_BUs != null) {
                if (!m_gemFM.c_BUs.isEmpty()) {
                    try {
                        logger.info(msgPrefix + "Trying to stop BU resources on stop.");
                        m_gemFM.c_BUs.execute(GEMInputs.STOP);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "Caught QualifiedResourceContainerException";
                        logger.error(msgPrefix + msg, e);
                        //m_gemFM.sendCMSError(msg);
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                    }
                }
            }

            if (m_gemFM.c_Ferols != null) {
                if (!m_gemFM.c_Ferols.isEmpty()) {
                    try {
                        logger.info(msgPrefix + "Trying to stop Ferol resources on stop.");
                        m_gemFM.c_Ferols.execute(GEMInputs.STOP);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = "Caught QualifiedResourceContainerException";
                        logger.error(msgPrefix + msg, e);
                        //m_gemFM.sendCMSError(msg);
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                    }
                }
            }

            // leave intermediate state
            m_gemFM.fireEvent( GEMInputs.SETCONFIGURED );

            // set action
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                new StringT("")));
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                new StringT("Stopping - Configured")));

            logger.debug("stopAction Executed");
        }
    }

    public void resumeAction(Object obj)
        throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::resmeAction(): ";

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
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                new StringT("Resume called")));
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                new StringT("Resuming")));

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            if (m_gemFM.c_gemSupervisors != null) {
                if (!m_gemFM.c_gemSupervisors.isEmpty()) {
                    XDAQParameter pam = null;
                    // prepare and set for all GEM supervisors the RunType
                    for (QualifiedResource qr : m_gemFM.c_gemSupervisors.getApplications() ){
                        try {
                            pam = ((XdaqApplication)qr).getXDAQParameter();
                            pam.select(new String[] {"RunNumber"});
                            pam.get();
                            String superRunNumber = pam.getValue("RunNumber");
                            logger.info(msgPrefix + "Obtained run number from supervisor: " + superRunNumber);
                            pam.setValue("RunNumber",m_gemFM.RunNumber.toString());
                            pam.send();
                        } catch (XDAQTimeoutException e) {
                            String msg = "Error! XDAQTimeoutException: startAction() when "
                                + " trying to send the m_gemFM.RunNumber to the GEM supervisor\n Perhaps this "
                                + "application is dead!?";
                            m_gemFM.goToError(msg, e);
                            logger.error(msgPrefix + msg);
                        } catch (XDAQException e) {
                            String msg = "Error! XDAQException: startAction() when trying "
                                + "to send the m_gemFM.RunNumber to the GEM supervisor";
                            m_gemFM.goToError(msg, e);
                            logger.error(msgPrefix + msg);
                        }
                    }

                    try {
                        logger.info(msgPrefix + "Trying to start GEMSupervisor.");
                        m_gemFM.c_gemSupervisors.execute(GEMInputs.START);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = e.getMessage();
                        logger.error(msgPrefix + msg);
                        //m_gemFM.sendCMSError(msg);
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                    }
                }
            }

            /*
            // RESUME? ferol/EVM/BU/RU?
            // need to resume first the EVM then BU and then the FerolController
            if (m_gemFM.c_uFEDKIT != null) {
            if (!m_gemFM.c_uFEDKIT.isEmpty()) {
            try {
            logger.info(msgPrefix + "Trying to enable uFEDKIT resources on start.");
            m_gemFM.c_uFEDKIT.execute(GEMInputs.ENABLE);
            } catch (QualifiedResourceContainerException e) {
            String msg = "Caught QualifiedResourceContainerException";
            logger.error(msgPrefix + msg, e);
            m_gemFM.goToError(msg, e);
            // m_gemFM.sendCMSError(msg);  // when do this rather than goToError, or both?
            m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
            m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
            }
            }
            }
            */

            // RESUME TCDS
            if (m_gemFM.c_tcdsControllers != null) {
                if (!m_gemFM.c_tcdsControllers.isEmpty()) {
                    try {
                        logger.info(msgPrefix + "Trying to enable TCDS resources on resume.");
                        m_gemFM.enableTCDSControllers();
                    } catch (UserActionException e) {
                        String msg = e.getMessage();
                        logger.error(msgPrefix + msg);
                        m_gemFM.goToError(msg, e);
                        // m_gemFM.sendCMSError(msg);  // when do this rather than goToError, or both?
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                    }
                }
            }

            // leave intermediate state
            m_gemFM.fireEvent( m_gemFM.hasSoftError() ? GEMInputs.SETRESUMEDSOFTERRORDETECTED :
                               ( m_gemFM.isDegraded() ? GEMInputs.SETRESUMEDDEGRADED : GEMInputs.SETRESUMED ));


            // Clean-up of the Function Manager parameters
            cleanUpFMParameters();

            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                new StringT("")));
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                new StringT("Resuming - Running")));

            logger.debug("resumeAction Executed");
        }
    }

    public void haltAction(Object obj)
        throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::haltAction(): ";

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
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                new StringT("Requested to halt")));
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,
                                                                new StringT("Halting")));

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            // halt TCDS
            if (m_gemFM.c_tcdsControllers != null) {
                if (!m_gemFM.c_tcdsControllers.isEmpty()) {
                    try {
                        logger.info(msgPrefix + "Trying to halt TCDS on halt.");
                        m_gemFM.haltTCDSControllers();
                    } catch (UserActionException e) {
                        String msg = e.getMessage();
                        m_gemFM.goToError(msg, e);
                        // m_gemFM.sendCMSError(msg);  // when do this rather than goToError, or both?
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                        logger.error(msgPrefix + msg);
                    }
                }
            }

            // halt GEMFSMApplications
            if (m_gemFM.c_gemSupervisors != null) {
                if (!m_gemFM.c_gemSupervisors.isEmpty()) {
                    try {
                        logger.info(msgPrefix + "Trying to halt GEMSupervisor.");
                        m_gemFM.c_gemSupervisors.execute(GEMInputs.HALT);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = e.getMessage();
                        m_gemFM.goToError(msg, e);
                        // m_gemFM.sendCMSError(msg);  // when do this rather than goToError, or both?
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                        logger.error(msgPrefix + msg);
                    }
                }
            }

            // ? ferol/EVM/BU/RU?
            if (m_gemFM.c_uFEDKIT != null) {
                if (!m_gemFM.c_uFEDKIT.isEmpty()) {
                    try {
                        logger.info(msgPrefix + "Trying to halt uFEDKIT resources on halt.");
                        m_gemFM.c_uFEDKIT.execute(GEMInputs.HALT);
                    } catch (QualifiedResourceContainerException e) {
                        String msg = e.getMessage();
                        m_gemFM.goToError(msg, e);
                        // m_gemFM.sendCMSError(msg);  // when do this rather than goToError, or both?
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("STATE",new StringT("Error")));
                        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT(msg)));
                        logger.error(msgPrefix + msg);
                    }
                }
            }

            // check from which state we came.
            if (m_gemFM.getPreviousState().equals(GEMStates.TTSTEST_MODE)) {
                // when we came from TTSTestMode we need to
                // 1. give back control of sTTS to HW
            }


            // leave intermediate state
            m_gemFM.fireEvent( GEMInputs.SETHALTED );

            // Clean-up of the Function Manager parameters
            cleanUpFMParameters();

            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("")));
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.STATE,new StringT("Halted")));

            logger.debug(msgPrefix + "haltAction Executed");
        }
    }

    public void preparingTTSTestModeAction(Object obj)
        throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::preparingTTSTestModeAction(): ";

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
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("preparingTestMode")));

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/
            // to prepare test we need to
            // 1. configure & enable fed application
            // 2. take control of fed

            // leave intermediate state
            m_gemFM.fireEvent( GEMInputs.SETTTSTEST_MODE );

            // set action
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("")));

            logger.debug("preparingTestModeAction Executed");
        }
    }

    public void testingTTSAction(Object obj)
        throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::testingTTSAction(): ";

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
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("testing TTS")));

            // get the parameters of the command
            ParameterSet<CommandParameter> inputParSet = m_gemFM.getLastInput().getParameterSet();

            // check parameter set
            if (inputParSet.size()==0 || inputParSet.get(GEMParameters.TTS_TEST_FED_ID) == null ||
                inputParSet.get(GEMParameters.TTS_TEST_MODE) == null ||
                ((StringT)inputParSet.get(GEMParameters.TTS_TEST_MODE).getValue()).equals("") ||
                inputParSet.get(GEMParameters.TTS_TEST_PATTERN) == null ||
                ((StringT)inputParSet.get(GEMParameters.TTS_TEST_PATTERN).getValue()).equals("") ||
                inputParSet.get(GEMParameters.TTS_TEST_SEQUENCE_REPEAT) == null)
                {
                    // go to error, we require parameters
                    String msg = "testingTTSAction: no parameters given with TestTTS command.";

                    // log error
                    logger.error(msg);

                    // notify error
                    sendCMSError(msg);

                    //go to error state
                    m_gemFM.fireEvent( GEMInputs.SETERROR );

                }

            Integer fedId  = ((IntegerT)inputParSet.get(GEMParameters.TTS_TEST_FED_ID).getValue()).getInteger();
            String mode    = ((StringT)inputParSet.get(GEMParameters.TTS_TEST_MODE).getValue()).getString();
            String pattern = ((StringT)inputParSet.get(GEMParameters.TTS_TEST_PATTERN).getValue()).getString();
            Integer cycles = ((IntegerT)inputParSet.get(GEMParameters.TTS_TEST_SEQUENCE_REPEAT).getValue()).getInteger();



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
            m_gemFM.fireEvent( GEMInputs.SETTTSTEST_MODE );

            // set action
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("")));

            logger.debug("preparingTestModeAction Executed");
        }
    }

    public void coldResettingAction(Object obj)
        throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::coldResettingAction(): ";

        if (obj instanceof StateNotification) {
            // triggered by State Notification from child resource

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            logger.info(msgPrefix + "Recieved ColdResetting state notification");

            return;
        } else if (obj instanceof StateEnteredEvent) {
            System.out.println("Executing coldResettingAction");
            logger.info("Executing coldResettingAction");

            // set action
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                new StringT("coldResetting")));

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/
            // perform a cold-reset of your hardware

            // ? TCDS
            // ? ferol/EVM/BU/RU?
            // ? GEMFSMApplications

            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                                new StringT("Cold Reset completed.")));
            // leave intermediate state
            m_gemFM.fireEvent( GEMInputs.SETHALTED );

            logger.debug("coldResettingAction Executed");
        }
    }

    public void fixSoftErrorAction(Object obj)
        throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::fixSoftErrorAction(): ";

        if (obj instanceof StateNotification) {
            // triggered by State Notification from child resource

            /************************************************
             * PUT YOUR CODE HERE
             ***********************************************/

            logger.info(msgPrefix + "Recieved FixSoftError state notification");

            return;
        } else if (obj instanceof StateEnteredEvent) {
            System.out.println("Executing fixSoftErrorAction");
            logger.info("Executing fixSoftErrorAction");

            // set action
            m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("fixingSoftError")));

            // get the parameters of the command
            ParameterSet<CommandParameter> inputParSet = m_gemFM.getLastInput().getParameterSet();

            // check parameter set
            Long triggerNumberAtPause = null;
            if (inputParSet.size()==0 || inputParSet.get(GEMParameters.TRIGGER_NUMBER_AT_PAUSE) == null) {

                // go to error, we require parameters
                String warnMsg = "fixSoftErrorAction: no parameters given with fixSoftError command.";

                // log error
                logger.warn(warnMsg);

            } else {
                triggerNumberAtPause = ((LongT)inputParSet.get(GEMParameters.TRIGGER_NUMBER_AT_PAUSE).getValue()).getLong();
            }

            /************************************************
             * PUT YOUR CODE HERE TO FIX THE SOFT ERROR
             ***********************************************/

            // ? TCDS
            // ? ferol/EVM/BU/RU?
            // ? GEMFSMApplications

            m_gemFM.setSoftErrorDetected(false);


            // if the soft error cannot be fixed, the FM should go to ERROR

            if (m_gemFM.hasSoftError())
                m_gemFM.fireEvent(  GEMInputs.SETERROR  );
            else
                m_gemFM.fireEvent(  m_gemFM.isDegraded() ? GEMInputs.SETRUNNINGDEGRADED : GEMInputs.SETRUNNING  );

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
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::runningDegradedAction(): ";

        if (obj instanceof StateEnteredEvent) {
            m_gemFM.setDegraded(true);
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
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::runningSoftErrorDetectedAction(): ";

        if (obj instanceof StateEnteredEvent) {
            logger.info(msgPrefix + "Recieved RunningSoftErrorDetected state notification");

            // do not touch degraded
            m_gemFM.setSoftErrorDetected(true);
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
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::runningAction(): ";

        if (obj instanceof StateEnteredEvent) {
            logger.info(msgPrefix + "Recieved Running state notification");

            m_gemFM.setDegraded(false);
            m_gemFM.setSoftErrorDetected(false);
            // ? TCDS
            // ? ferol/EVM/BU/RU?
            // ? GEMFSMApplications
        }
    }

    // This is duplicated from GEMFunctionManger --- why?
    @SuppressWarnings("unchecked")
	private void sendCMSError(String errMessage)
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::sendCMSError(): ";

        // create a new error notification msg
        CMSError error = m_gemFM.getErrorFactory().getCMSError();
        error.setDateTime(new Date().toString());
        error.setMessage(errMessage);

        // update error  parameter for GUI
        m_gemPSet.get(GEMParameters.ERROR_MSG).setValue(new StringT(errMessage));

        // send error
        try {
            m_gemFM.getParentErrorNotifier().sendError(error);
        } catch (Exception e) {
            logger.warn(msgPrefix + "Failed to send error mesage " + errMessage);
        }
    }

    private void cleanUpFMParameters()
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::cleanUpFMParameters(): ";

        // Clean-up of the Function Manager parameters
        m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,
                                                            new StringT("")));
        m_gemPSet.put(new FunctionManagerParameter<StringT>(GEMParameters.ERROR_MSG,
                                                            new StringT("")));
        m_gemPSet.put(new FunctionManagerParameter<IntegerT>(GEMParameters.TTS_TEST_FED_ID,
                                                             new IntegerT(-1)));
    }

    protected void initXDAQ()
        throws UserActionException
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::initXDAQ(): ";

        // Look if the configuration uses TCDS and handle accordingly.
        // First check if TCDS is being used, and if so, tell RCMS that the TCDS executives are already initialized.
        String msg = "Initializing XDAQ applications...";
        logger.info(msgPrefix + msg);
        Boolean usingTCDS = false;
        // QualifiedGroup qg = m_gemFM.getQualifiedGroup();

        Object sidObj = m_gemQG.getRegistryEntry(GEMParameters.SID);
        if (sidObj!=null) {
            logger.info(msgPrefix + "GEMEventHandler::initXDAQ() - SID object is " + sidObj.toString());
        } else {
            logger.warn(msgPrefix + "GEMEventHandler::initXDAQ() - SID object is null. "
                        + "This is OK if LV2 has not received it from LV1.");
        }

        List<QualifiedResource> xdaqExecList = m_gemQG.seekQualifiedResourcesOfType(new XdaqExecutive());
        m_gemFM.c_xdaqExecs                  = new XdaqApplicationContainer(xdaqExecList);

        // Always set TCDS executive and xdaq apps to initialized and the associated jobcontrol to Active=false
        maskTCDSExecAndJC(m_gemQG);

        for (QualifiedResource qr : xdaqExecList) {
            String hostName = qr.getResource().getHostName();
            // ===WARNING!!!=== This hostname is hardcoded and should NOT be!!!
            // TODO This needs to be moved out into userXML or a snippet!!!
            if (hostName.contains("tcds")) {
                usingTCDS = true;
                msg = "the TCDS executive on hostName " + hostName + " is being handled in a special way.";
                logger.info(msgPrefix + msg);
                qr.setInitialized(true);
            }
        }

        // find xdaq applications
        List<QualifiedResource> jcList = m_gemQG.seekQualifiedResourcesOfType(new JobControl());
        m_gemFM.c_JCs                  = new XdaqApplicationContainer(jcList);

        for (QualifiedResource qr: jcList) {
            if (qr.getResource().getHostName().contains("tcds")) {
                msg = "Masking the  application with name " + qr.getName()
                    + " running on host " + qr.getResource().getHostName();
                logger.info(msgPrefix + msg);
                qr.setActive(false);
            }
        }

        logger.info("[GEM "+ m_gemFM.m_FMname + "] SID of QG is " + m_gemQG.getRegistryEntry(GEMParameters.SID));
        if ( m_gemQG.getRegistryEntry(GEMParameters.SID) == null) {
            Integer sid = ((IntegerT)m_gemPSet.get(GEMParameters.SID).getValue()).getInteger();
            m_gemQG.putRegistryEntry(GEMParameters.SID, sid.toString());
            logger.info("[GEM "+ m_gemFM.m_FMname + "] Just set the SID of QG to " + sid);
        } else {
            logger.info("[GEM "+ m_gemFM.m_FMname + "] SID of QG is "
                        + m_gemQG.getRegistryEntry(GEMParameters.SID));
        }
        // m_gemFM.setQualifiedGroup(qg);
        logger.info("[GEM "+ m_gemFM.m_FMname + "] SID of QG is "
                    + m_gemFM.getQualifiedGroup().getRegistryEntry(GEMParameters.SID));

        // Start by getting XdaqServiceApplications
        List<QualifiedResource> xdaqServiceAppList = m_gemQG.seekQualifiedResourcesOfType(new XdaqServiceApplication());
        m_gemFM.c_xdaqServiceApps                  = new XdaqApplicationContainer(xdaqServiceAppList);

        List<String> applicationClasses = m_gemFM.c_xdaqServiceApps.getApplicationClasses();
        for (String cla : applicationClasses) {
            msg = "found service application class: " + cla;
            logger.info(msgPrefix + msg);
        }

        // TCDS apps -> Needs to be defined for GEM
        logger.info(msgPrefix + "Looking for TCDS");
        logger.info(msgPrefix + "Getting all LPMControllers");
        List<XdaqApplication> lpmList  = m_gemFM.c_xdaqServiceApps
            .getApplicationsOfClass("tcds::lpm::LPMController");
        logger.info(msgPrefix + "Getting all ICIControllers");
        List<XdaqApplication> iciList  = m_gemFM.c_xdaqServiceApps
            .getApplicationsOfClass("tcds::ici::ICIController");
        logger.info(msgPrefix + "Getting all PIControllers");
        List<XdaqApplication> piList   = m_gemFM.c_xdaqServiceApps
            .getApplicationsOfClass("tcds::pi::PIController"  );
        logger.info(msgPrefix + "Making the TCDS list");
        List<XdaqApplication> tcdsList = new ArrayList<XdaqApplication>();

        logger.info(msgPrefix + "Adding TCDS applications to TCDS list");
        tcdsList.addAll(lpmList);
        tcdsList.addAll(iciList);
        tcdsList.addAll(piList);

        logger.info(msgPrefix + "Creating the TCDS containers");
        m_gemFM.c_tcdsControllers = new XdaqApplicationContainer(tcdsList);
        m_gemFM.c_lpmControllers  = new XdaqApplicationContainer(lpmList);
        m_gemFM.c_iciControllers  = new XdaqApplicationContainer(iciList);
        m_gemFM.c_piControllers   = new XdaqApplicationContainer(piList);

        // Now if we are using TCDS, give all of the TCDS applications the URN that they need.

        try {
            msg = "initializing the qualified group and printing the qualifed group\n";
            logger.info(msgPrefix + msg + m_gemQG.print());
            m_gemQG.init();
        } catch (Exception e) {
            // failed to init
            StringWriter sw = new StringWriter();
            e.printStackTrace( new PrintWriter(sw) );
            System.out.println(sw.toString());
            msg = this.getClass().toString() + " failed to initialize resources. Printing stacktrace: " + sw.toString();
            logger.error(msgPrefix + msg, e);
            throw new UserActionException(e.getMessage());
        }

        msg = "initialized the qualified group";
        logger.info(msgPrefix + msg);

        // Now, find xdaq applications
        List<QualifiedResource> xdaqAppList = m_gemQG.seekQualifiedResourcesOfType(new XdaqApplication());
        m_gemFM.c_xdaqApps                  = new XdaqApplicationContainer(xdaqAppList);

        applicationClasses = m_gemFM.c_xdaqApps.getApplicationClasses();
        for (String cla : applicationClasses) {
            msg = "found application class: " + cla;
            logger.info(msgPrefix + msg);
        }

        msg = xdaqAppList.size() + " XDAQ applications controlled, "
            + xdaqServiceAppList.size() + " XDAQ service applications used";
        logger.debug(msgPrefix + msg);

        // fill applications
        msg = "Retrieving GEM XDAQ applications ...";
        logger.info(msgPrefix + ""  + msg);
        m_gemPSet.put(new FunctionManagerParameter<StringT>("ACTION_MSG", new StringT(msg)));

        m_gemFM.c_gemSupervisors =
            new XdaqApplicationContainer(m_gemFM.c_xdaqApps
                                         .getApplicationsOfClass("gem::supervisor::GEMSupervisor"));
        if (!m_gemFM.c_gemSupervisors.isEmpty()) {
            logger.info(msgPrefix + "GEM supervisor found! Welcome to GEMINI XDAQ control :)");
        } else {
            logger.info(msgPrefix + "GEM supervisor was not found!");
        }

        // Applications related to uFEDKIT readout
        List<XdaqApplication> fedKitList = new ArrayList<XdaqApplication>();

        // Ferol apps -> Needs to be defined for GEM
        logger.info(msgPrefix + "Looking for ferol");
        List<XdaqApplication> ferolList = m_gemFM.c_xdaqApps.getApplicationsOfClass("ferol::FerolController");
        m_gemFM.c_Ferols                = new XdaqApplicationContainer(ferolList);

        // evb apps -> Needs to be defined for GEM
        logger.info(msgPrefix + "Looking for evm");
        List<XdaqApplication> buList  = m_gemFM.c_xdaqApps.getApplicationsOfClass("evb::BU");
        List<XdaqApplication> ruList  = m_gemFM.c_xdaqApps.getApplicationsOfClass("evb::RU");
        List<XdaqApplication> evmList = m_gemFM.c_xdaqApps.getApplicationsOfClass("evb::EVM");

        m_gemFM.c_BUs  = new XdaqApplicationContainer(buList);
        m_gemFM.c_RUs  = new XdaqApplicationContainer(ruList);
        m_gemFM.c_EVMs = new XdaqApplicationContainer(evmList);


        fedKitList.addAll(ferolList);
        fedKitList.addAll(buList);
        fedKitList.addAll(ruList);
        fedKitList.addAll(evmList);
        m_gemFM.c_uFEDKIT = new XdaqApplicationContainer(fedKitList);

        // find out if GEM supervisor is ready for async SOAP communication
        if (!m_gemFM.c_gemSupervisors.isEmpty()) {
            logger.info(msgPrefix + "initXDAQ checking async SOAP communication with GEMSupervisor");

            XDAQParameter pam = null;

            String dowehaveanasyncgemSupervisor = "undefined";

            logger.info(msgPrefix + "looping over qualified resources of GEMSupervisor type");
            // ask for the status of the GEM supervisor and wait until it is Ready or Failed
            for (QualifiedResource qr : m_gemFM.c_gemSupervisors.getApplications() ){
                logger.info(msgPrefix + "found qualified resource of GEMSupervisor type");
                try {
                    logger.info(msgPrefix + "trying to get parameters");
                    pam =((XdaqApplication)qr).getXDAQParameter();
                    logger.info(msgPrefix + "got parameters, selecting:");

                    pam.select(new String[] {"TriggerAdapterName", "PartitionState", "InitializationProgress","ReportStateToRCMS"});
                    logger.info(msgPrefix + "parameters selected, getting:");
                    pam.get();
                    logger.info(msgPrefix + "got selectedparameters!");

                    dowehaveanasyncgemSupervisor = pam.getValue("ReportStateToRCMS");
                    msg = "asking for the GEM supervisor ReportStateToRCMS results is: " + dowehaveanasyncgemSupervisor;
                    logger.info(msgPrefix + msg);

                } catch (XDAQTimeoutException e) {
                    msg = "Error! XDAQTimeoutException in initXDAQ() when checking the async SOAP capabilities\n"
                        + "Perhaps the GEMSupervisor application is dead!?";
                    //m_gemFM.goToError(msg, e);
                    logger.error(msgPrefix + msg, e);
                } catch (XDAQException e) {
                    msg = "Error! XDAQException in initXDAQ() when checking the async SOAP capabilities";
                    //m_gemFM.goToError(msg, e);
                    logger.error(msgPrefix + msg, e);
                }

                logger.info(msgPrefix + "using async SOAP communication with GEMSupervisor ...");
            }
        } else {
            msg = "Warning! No GEM supervisor found in initXDAQ().\n"
                + "This happened when checking the async SOAP capabilities.\n"
                + "This is OK for a level1 FM.";
            logger.warn(msgPrefix + msg);
        }
        /*
        // finally, halt all LPM apps
        m_gemFM.haltLPMControllers();

        // define the condition state vectors only here since the group must have been qualified before and all containers are filled
        m_gemFM.defineConditionState();
        m_gemFM.getGEMParameterSet().put(new FunctionManagerParameter<StringT>("ACTION_MSG",new StringT("")));
        */
    }

    void maskTCDSExecAndJC(QualifiedGroup qg)
    {
        String msgPrefix = "[GEM FM::" + m_gemFM.m_FMname + "] GEMEventHandler::maskTCDSExecAndJC(): ";

        logger.info(msgPrefix + "masking TCDS resources and setting their state to initialized");

        // mark TCDS execs as initialized and mask their JobControl
        List<QualifiedResource> xdaqExecList        = m_gemQG.seekQualifiedResourcesOfType(new XdaqExecutive());
        // why the duplicate
        List<QualifiedResource> xdaqServiceAppsList = m_gemQG.seekQualifiedResourcesOfType(new XdaqServiceApplication());
        // is this even needed?
        List<QualifiedResource> xdaqAppsList        = m_gemQG.seekQualifiedResourcesOfType(new XdaqApplication());

        //In case we turn TCDS to service app on-the-fly in the future...
        xdaqAppsList.addAll(xdaqServiceAppsList);

        // mask TCDS executive resources by hostname, role, or application name
        for (QualifiedResource qr : xdaqExecList) {
            boolean foundTCDS = false;
            if (qr.getResource().getHostName().contains("tcds") ) {
                logger.info(msgPrefix + "TCDS found resource by hostname: "
                            + qr.getResource().getHostName());
                foundTCDS = true;
            } else if (qr.getResource().getRole().contains("tcds")) {
                logger.info(msgPrefix + "TCDS found resource by role: "
                            + qr.getResource().getRole());
                foundTCDS = true;
            } else if (qr.getResource().getName().contains("tcds")) {
                logger.info(msgPrefix + "TCDS found resource by name: "
                            + qr.getResource().getName());
                foundTCDS = true;
            }

            if (foundTCDS) {
                logger.info(msgPrefix + "found TCDS executive resource, masking resource and associated JC");
                qr.setInitialized(true);
                m_gemQG.seekQualifiedResourceOnPC(qr, new JobControl()).setActive(false);
            }
        }

        // mark TCDS apps as initialized by hostname, role or application name
        for (QualifiedResource qr : xdaqAppsList) {
            boolean foundTCDS = false;
            if (qr.getResource().getHostName().contains("tcds") ) {
                logger.info(msgPrefix + "TCDS application found resource by hostname: "
                            + qr.getResource().getHostName());
                foundTCDS = true;
            } else if (qr.getResource().getRole().contains("tcds")) {
                logger.info(msgPrefix + "TCDS application found resource by role: "
                            + qr.getResource().getRole());
                foundTCDS = true;
            } else if (qr.getResource().getName().contains("tcds")) {
                logger.info(msgPrefix + "TCDS application found resource by name: "
                            + qr.getResource().getName());
                foundTCDS = true;
            }

            if (foundTCDS) {
                logger.info(msgPrefix + "found TCDS application resource");
                qr.setInitialized(true);
            }
        }

        // mark TCDS apps as initialized by hostname, role or application name
        for (QualifiedResource qr : xdaqServiceAppsList) {
            boolean foundTCDS = false;
            if (qr.getResource().getHostName().contains("tcds") ) {
                logger.info(msgPrefix + "TCDS service application found resource by hostname: "
                            + qr.getResource().getHostName());
                foundTCDS = true;
            } else if (qr.getResource().getRole().contains("tcds")) {
                logger.info(msgPrefix + "TCDS service application found resource by role: "
                            + qr.getResource().getRole());
                foundTCDS = true;
            } else if (qr.getResource().getName().contains("tcds")) {
                logger.info(msgPrefix + "TCDS service application found resource by name: "
                            + qr.getResource().getName());
                foundTCDS = true;
            }

            if (foundTCDS) {
                logger.info(msgPrefix + "found TCDS service application resource");
                qr.setInitialized(true);
            }
        }
    }
}
