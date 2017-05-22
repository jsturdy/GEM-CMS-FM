package rcms.fm.app.gemfm;


import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Calendar;

import rcms.errorFormat.CMS.CMSError;
import rcms.fm.fw.StateEnteredEvent;
import rcms.fm.fw.parameter.CommandParameter;
import rcms.fm.fw.parameter.FunctionManagerParameter;
import rcms.fm.fw.parameter.ParameterSet;
import rcms.fm.fw.parameter.type.IntegerT;
import rcms.fm.fw.parameter.type.LongT;
import rcms.fm.fw.parameter.type.StringT;
import rcms.fm.fw.user.UserActionException;
import rcms.fm.fw.user.UserStateNotificationHandler;
import rcms.fm.resource.QualifiedGroup;
import rcms.fm.resource.QualifiedResource;
import rcms.fm.resource.qualifiedresource.XdaqApplication;
import rcms.fm.resource.qualifiedresource.XdaqApplicationContainer;
import rcms.stateFormat.StateNotification;
import rcms.util.logger.RCMSLogger;

import rcms.util.logsession.LogSessionException;
import rcms.util.logsession.LogSessionConnector;

import rcms.utilities.runinfo.RunNumberData;
import rcms.utilities.runinfo.RunSequenceNumber;
import rcms.utilities.runinfo.RunInfo;
import rcms.utilities.runinfo.RunInfoException;
import rcms.utilities.runinfo.RunInfoConnectorIF;

/**
 * 
 * Main Event Handler class for Level 1 Function Manager.
 * 
 * @author Andrea Petrucci, Alexander Oh, Michele Gulmini
 * @maintainer Jose Ruiz
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
        public String RunSequenceName   =  "HCAL test"; // Run sequence name, for attaining a run sequence number
	
	public GEMEventHandler() throws rcms.fm.fw.EventHandlerException {
		// this handler inherits UserStateNotificationHandler
		// so it is already registered for StateNotification events
		
		// Let's register also the StateEnteredEvent triggered when the FSM enters in a new state.
		subscribeForEvents(StateEnteredEvent.class);
		
		addAction(GEMStates.INITIALIZING,			"initAction");		
		addAction(GEMStates.CONFIGURING, 			"configureAction");
		addAction(GEMStates.HALTING,     			"haltAction");
		addAction(GEMStates.PREPARING_TTSTEST_MODE,	"preparingTTSTestModeAction");		
		addAction(GEMStates.TESTING_TTS,    			"testingTTSAction");		
		addAction(GEMStates.COLDRESETTING,           "coldResettingAction");		
		addAction(GEMStates.PAUSING,     			"pauseAction");
		addAction(GEMStates.RECOVERING,  			"recoverAction");
		addAction(GEMStates.RESETTING,   			"resetAction");
		addAction(GEMStates.RESUMING,    			"resumeAction");
		addAction(GEMStates.STARTING,    			"startAction");
		addAction(GEMStates.STOPPING,    			"stopAction");
	
		addAction(GEMStates.FIXINGSOFTERROR,    		"fixSoftErrorAction");
		
		addAction(GEMStates.RUNNINGDEGRADED, "runningDegradedAction");                    // for testing with external inputs
		addAction(GEMStates.RUNNINGSOFTERRORDETECTED, "runningSoftErrorDetectedAction");  // for testing with external inputs
		addAction(GEMStates.RUNNING, "runningAction");                                    // for testing with external inputs
	}
	
	
	public void init() throws rcms.fm.fw.EventHandlerException {
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
    Sid = ((IntegerT)functionManager.getParameterSet().get("SID").getValue()).getInteger();
    if ( ric == null ) {
      logger.error("[GEM " + functionManager.FMname + "] RunInfoConnector is empty i.e. Is there a RunInfo DB or is it down?");

      // by default give run number 0
      return new RunNumberData(new Integer(Sid),new Integer(0),functionManager.getOwner(),Calendar.getInstance().getTime());
    }
    else {
      RunSequenceNumber rsn = new RunSequenceNumber(ric,functionManager.getOwner(),RunSequenceName);
      RunNumberData rnd = rsn.createRunSequenceNumber(Sid);

      logger.info("[GEM " + functionManager.FMname + "] received run number: " + rnd.getRunNumber() + " and sequence number: " + rnd.getSequenceNumber());

      functionManager.GEMRunInfo = null; // make RunInfo ready for the next round of run info to store
      return rnd;
    }
  }

  // establish connection to RunInfoDB - if needed                                                                                                                                            
  protected void checkRunInfoDBConnection() {
    if (functionManager.GEMRunInfo == null) {
      logger.info("[GEM " + functionManager.FMname + "] creating new RunInfo accessor with namespace: " + functionManager.GEM_NS + " now ...");

      //Get SID from parameter                                                                                                                                                                
      Sid = ((IntegerT)functionManager.getParameterSet().get("SID").getValue()).getInteger();

      RunInfoConnectorIF ric = functionManager.getRunInfoConnector();
      functionManager.GEMRunInfo =  new RunInfo(ric,Sid,Integer.valueOf(functionManager.RunNumber));

      functionManager.GEMRunInfo.setNameSpace(functionManager.GEM_NS);

      logger.info("[GEM " + functionManager.FMname + "] ... RunInfo accessor available.");
    }
  }

	public void initAction(Object obj) throws UserActionException {
		
		if (obj instanceof StateNotification) {
			
			// triggered by State Notification from child resource
			
			/************************************************
			 * PUT YOUR CODE HERE							
			 ***********************************************/
			
			return;
		}
		
		else if (obj instanceof StateEnteredEvent) {
			
			// triggered by entered state action
			// let's command the child resources
			
			// debug
			logger.debug("initAction called.");
			
			// set action
			functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("Initializing")));
		
			// get the parameters of the command
			Integer sid;
			String globalConfKey = null;
			
			try {
				ParameterSet<CommandParameter> parameterSet = getUserFunctionManager().getLastInput().getParameterSet();
				if (parameterSet.get("SID") != null) {
				    sid = ((CommandParameter<IntegerT>)parameterSet.get(GEMParameters.SID)).getValue().getInteger();
				    ((FunctionManagerParameter<IntegerT>)functionManager.getParameterSet().get(GEMParameters.INITIALIZED_WITH_SID)).setValue(new IntegerT(sid));
				    logger.debug("[GEM INIT] INITIALIZED_WITH_SID has been set");
				    //getParameterSet().get("INITIALIZED_WITH_SID").setValue(new IntegerT(sid)); //For the moment this parameter is only here to show if it is correctly set after initialization -> Really needed in future?
				}
				else {
				    logger.debug("[GEM INIT] SID has been found to be null");
				}
				//globalConfKey = ((CommandParameter<StringT>)parameterSet.get(GEMParameters.GLOBAL_CONF_KEY)).getValue().toString();
			}
			catch (Exception e) {
				// go to error, we require parameters
				String errMsg = "initAction: error reading command parameters of Initialize command.";
				
				// log error
				logger.error(errMsg, e);
				
				// notify error
				sendCMSError(errMsg);
				
				//go to error state
				functionManager.fireEvent( GEMInputs.SETERROR );
				return;
			}
 			
				
			// 
			// initialize qualified group
			
			//
			/*QualifiedGroup qg = functionManager.getQualifiedGroup();

			try {
				qg.init();
			} catch (Exception e) {
				// failed to init
				String errMsg = this.getClass().toString() + " failed to initialize resources";
			
				// send error notification
				sendCMSError(errMsg);
		
				//log error
				logger.error(errMsg,e);
			
				// go to error state
				functionManager.fireEvent(GEMInputs.SETERROR);
				return;
			}


			// find xdaq applications
			List<QualifiedResource> xdaqList = qg.seekQualifiedResourcesOfType(new XdaqApplication());
			functionManager.containerXdaqApplication = new XdaqApplicationContainer(xdaqList);
			logger.debug("Application list : " + xdaqList.size() );
			*/
			// Example: find "your" applications
			// functionManager.containerYourClass = new XdaqApplicationContainer( 
			//		functionManager.containerXdaqApplication.getApplicationsOfClass("yourClass"));

			/************************************************
			 * PUT YOUR CODE HERE							
			 ***********************************************/

			// set exported parameters
			//((FunctionManagerParameter<IntegerT>)functionManager.getParameterSet().get(GEMParameters.INITIALIZED_WITH_SID)).setValue(new IntegerT(sid));
			//((FunctionManagerParameter<StringT>)functionManager.getParameterSet().get(GEMParameters.INITIALIZED_WITH_GLOBAL_CONF_KEY)).setValue(new StringT(globalConfKey));
			
			
			 // go to HALT
			functionManager.fireEvent( GEMInputs.SETHALTED );
			
			
			// set action
			functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("")));
			
			logger.info("initAction Executed");
		}
	}


	public void resetAction(Object obj) throws UserActionException {
		
		if (obj instanceof StateNotification) {
			
			// triggered by State Notification from child resource
			
			/************************************************
			 * PUT YOUR CODE HERE							
			 ***********************************************/
			
			return;
		}
		
		else if (obj instanceof StateEnteredEvent) {
			
						
				// triggered by entered state action
				// let's command the child resources
				
				// debug
				logger.debug("resetAction called.");
				
				// set action
				functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("Resetting")));
				
				/************************************************
				 * PUT YOUR CODE HERE							
				 ***********************************************/
				
				functionManager.GEMRunInfo = null; // make RunInfo ready for the next round of run info to store

				// go to Initital
				functionManager.fireEvent( GEMInputs.SETHALTED );
				
				// Clean-up of the Function Manager parameters
				cleanUpFMParameters();
				
				logger.info("resetAction Executed");
		}	
	}
	
	public void recoverAction(Object obj) throws UserActionException {
		
		if (obj instanceof StateNotification) {
			
			// triggered by State Notification from child resource
			
			/************************************************
			 * PUT YOUR CODE HERE							
			 ***********************************************/
					
			return;
		}
		
		else if (obj instanceof StateEnteredEvent) {
			
				System.out.println("Executing recoverAction");
				logger.info("Executing recoverAction");
				
				// set action
				functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("recovering")));
				
				/************************************************
				 * PUT YOUR CODE HERE							
				 ***********************************************/
				
				// leave intermediate state
				functionManager.fireEvent( GEMInputs.SETHALTED );
				
				// Clean-up of the Function Manager parameters
				cleanUpFMParameters();
				
				logger.info("recoverAction Executed");
				}
	}
	
	public void configureAction(Object obj) throws UserActionException {
		
		if (obj instanceof StateNotification) {

			// triggered by State Notification from child resource
			
			/************************************************
			 * PUT YOUR CODE HERE							
			 ***********************************************/

			return;
		}
		
		else if (obj instanceof StateEnteredEvent) {
			System.out.println("Executing configureAction");
			logger.info("Executing configureAction");
			
			// set action
			functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("configuring")));
			
			// get the parameters of the command
			//Integer runNumber;
			//String runKey = null;
			//String fedEnableMask = null;
			
			try {
				ParameterSet<CommandParameter> parameterSet = getUserFunctionManager().getLastInput().getParameterSet();
				//runNumber = ((CommandParameter<IntegerT>)parameterSet.get(GEMParameters.RUN_NUMBER)).getValue().getInteger();
				//runKey = ((CommandParameter<StringT>)parameterSet.get(GEMParameters.RUN_KEY)).getValue().toString();
				//fedEnableMask = ((CommandParameter<StringT>)parameterSet.get(GEMParameters.FED_ENABLE_MASK)).getValue().toString();
			}
			catch (Exception e) {
				// go to error, we require parameters
				String errMsg = "configureAction: error reading command parameters of Configure command.";
				
				// log error
				logger.error(errMsg, e);
				
				// notify error
				sendCMSError(errMsg);
				
				//go to error state
				functionManager.fireEvent( GEMInputs.SETERROR );
				return;
			}			
			
			/************************************************
			 * PUT YOUR CODE HERE							
			 ***********************************************/
			
			// Set the configuration parameters in the Function Manager parameters
			//((FunctionManagerParameter<IntegerT>)functionManager.getParameterSet().get(GEMParameters.CONFIGURED_WITH_RUN_NUMBER)).setValue(new IntegerT(runNumber));
			//((FunctionManagerParameter<StringT>)functionManager.getParameterSet().get(GEMParameters.CONFIGURED_WITH_RUN_KEY)).setValue(new StringT(runKey));
			//((FunctionManagerParameter<StringT>)functionManager.getParameterSet().get(GEMParameters.CONFIGURED_WITH_FED_ENABLE_MASK)).setValue(new StringT(fedEnableMask));

			// leave intermediate state
			functionManager.fireEvent( GEMInputs.SETCONFIGURED );
			
			// set action
			functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("")));

			
			
			logger.info("configureAction Executed");
		}
	}
	
	public void startAction(Object obj) throws UserActionException {
		
		if (obj instanceof StateNotification) {
			
			// triggered by State Notification from child resource
			
			/************************************************
			 * PUT YOUR CODE HERE							
			 ***********************************************/

			return;
		}
		
		else if (obj instanceof StateEnteredEvent) {
			System.out.println("Executing startAction");
			logger.info("Executing startAction");
			
			// set action
			functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("starting")));

			// get the parameters of the command
			ParameterSet<CommandParameter> parameterSet = getUserFunctionManager().getLastInput().getParameterSet();

			if(functionManager.getRunInfoConnector()!=null){
			    RunNumberData rnd = getOfficialRunNumber();
			    
			    functionManager.RunNumber    = rnd.getRunNumber();
			    //RunSeqNumber = rnd.getSequenceNumber();
			    
			    functionManager.getParameterSet().put(new FunctionManagerParameter<IntegerT>("RUN_NUMBER", new IntegerT(functionManager.RunNumber)));
			    //functionManager.getGEMparameterSet().put(new FunctionManagerParameter<IntegerT>("RUN_SEQ_NUMBER", new IntegerT(RunSeqNumber)));
			    //logger.info("[GEM LVL1 " + functionManager.FMname + "] ... run number: " + functionManager.RunNumber + ", SequenceNumber: " + RunSeqNumber);
			}
			else{
			    logger.error("[GEM LVL1 "+functionManager.FMname+"] Official RunNumber requested, but cannot establish RunInfo Connection. Is there a RunInfo DB? or is RunInfo DB down?");
			    //logger.info("[GEM LVL1 "+functionManager.FMname+"] Going to use run number ="+functionManager.RunNumber+", RunSeqNumber = "+ RunSeqNumber);
			}

			// check parameter set
			if (parameterSet.size()==0 || parameterSet.get(GEMParameters.RUN_NUMBER) == null )  {

				// go to error, we require parameters
				String errMsg = "startAction: no parameters given with start command.";
				
				// log error
				logger.error(errMsg);
				
				// notify error
				sendCMSError(errMsg);
				
				// go to error state
				functionManager.fireEvent( GEMInputs.SETERROR );
				return;
			}
			
			// get the run number from the start command
			Integer runNumber = ((IntegerT)parameterSet.get(GEMParameters.RUN_NUMBER).getValue()).getInteger();

			functionManager.getParameterSet().put(new FunctionManagerParameter<IntegerT>(GEMParameters.STARTED_WITH_RUN_NUMBER,new IntegerT(runNumber)));

                        // Set the run number in the Function Manager parameters             
                        functionManager.getParameterSet().put(new FunctionManagerParameter<IntegerT>(GEMParameters.RUN_NUMBER,new IntegerT(runNumber)));

			/************************************************
			 * PUT YOUR CODE HERE							
			 ***********************************************/
			
			// Set the run number in the Function Manager parameters
			//((FunctionManagerParameter<IntegerT>)functionManager.getParameterSet().get(GEMParameters.STARTED_WITH_RUN_NUMBER)).setValue(new IntegerT(runNumber));

			// leave intermediate state
			functionManager.fireEvent( GEMInputs.SETRUNNING );
			
			// set action
			functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("")));

			
			
			logger.debug("startAction Executed");
			
		}
	}
	
	public void pauseAction(Object obj) throws UserActionException {
		
		if (obj instanceof StateNotification) {
			
			// triggered by State Notification from child resource
			
			/************************************************
			 * PUT YOUR CODE HERE							
			 ***********************************************/

			return;
		}
		
		else if (obj instanceof StateEnteredEvent) {
			
			System.out.println("Executing pauseAction");
			logger.info("Executing pauseAction");
			
			// set action
			functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("pausing")));
			
			/************************************************
			 * PUT YOUR CODE HERE							
			 ***********************************************/
			
			// leave intermediate state
			functionManager.fireEvent( GEMInputs.SETPAUSED );
			
			// set action
			functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("")));
			
			logger.debug("pausingAction Executed");
			
		}
	}
	
	public void stopAction(Object obj) throws UserActionException {
		
		if (obj instanceof StateNotification) {
			
			// triggered by State Notification from child resource
			
			/************************************************
			 * PUT YOUR CODE HERE							
			 ***********************************************/

			return;
		}
		
		else if (obj instanceof StateEnteredEvent) {	
			System.out.println("Executing stopAction");
			logger.info("Executing stopAction");
			
			// set action
			functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("stopping")));
			
			/************************************************
			 * PUT YOUR CODE HERE							
			 ***********************************************/
			
			// leave intermediate state
			functionManager.fireEvent( GEMInputs.SETCONFIGURED );
			
			// set action
			functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("")));
			
			logger.debug("stopAction Executed");
			
		}
	}

	public void resumeAction(Object obj) throws UserActionException {
		
		if (obj instanceof StateNotification) {
			
			// triggered by State Notification from child resource
			
			/************************************************
			 * PUT YOUR CODE HERE							
			 ***********************************************/

			return;
		}
		
		else if (obj instanceof StateEnteredEvent) {	
			System.out.println("Executing resumeAction");
			logger.info("Executing resumeAction");
			
			// set action
			functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("resuming")));
			
			/************************************************
			 * PUT YOUR CODE HERE							
			 ***********************************************/
			
			// leave intermediate state
			functionManager.fireEvent( functionManager.hasSoftError() ? GEMInputs.SETRESUMEDSOFTERRORDETECTED :
				                       ( functionManager.isDegraded() ? GEMInputs.SETRESUMEDDEGRADED : GEMInputs.SETRESUMED )  );
			
			
			// Clean-up of the Function Manager parameters
			cleanUpFMParameters();
		
			logger.debug("resumeAction Executed");
			
		}
	}
	
	public void haltAction(Object obj) throws UserActionException {
		
		if (obj instanceof StateNotification) {
			
			// triggered by State Notification from child resource
			
			/************************************************
			 * PUT YOUR CODE HERE							
			 ***********************************************/
			
			return;
		}
		
		else if (obj instanceof StateEnteredEvent) {
			System.out.println("Executing haltAction");
			logger.info("Executing haltAction");
			
			// set action
			functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("halting")));
			
			/************************************************
			 * PUT YOUR CODE HERE							
			 ***********************************************/

			// check from which state we came.
			if (functionManager.getPreviousState().equals(GEMStates.TTSTEST_MODE)) {
				// when we came from TTSTestMode we need to
				// 1. give back control of sTTS to HW
			}
			
			
			// leave intermediate state
			functionManager.fireEvent( GEMInputs.SETHALTED );
			
			// Clean-up of the Function Manager parameters
			cleanUpFMParameters();
			
			logger.debug("haltAction Executed");
		}
	}	
	
	public void preparingTTSTestModeAction(Object obj) throws UserActionException {

		if (obj instanceof StateNotification) {

			// triggered by State Notification from child resource

			/************************************************
			 * PUT YOUR CODE HERE							
			 ***********************************************/

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
	
	public void testingTTSAction(Object obj) throws UserActionException {


		XdaqApplication fmm = null;
		Map attributeMap = new HashMap();

		if (obj instanceof StateNotification) {

			// triggered by State Notification from child resource

			/************************************************
			 * PUT YOUR CODE HERE							
			 ***********************************************/

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
			
			Integer fedId = ((IntegerT)parameterSet.get(GEMParameters.TTS_TEST_FED_ID).getValue()).getInteger();
			String mode = ((StringT)parameterSet.get(GEMParameters.TTS_TEST_MODE).getValue()).getString();
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

	public void coldResettingAction(Object obj) throws UserActionException {

		if (obj instanceof StateNotification) {

			// triggered by State Notification from child resource

			/************************************************
			 * PUT YOUR CODE HERE							
			 ***********************************************/

			return;
		}

		else if (obj instanceof StateEnteredEvent) {
			System.out.println("Executing coldResettingAction");
			logger.info("Executing coldResettingAction");

			// set action
			functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("coldResetting")));

			/************************************************
			 * PUT YOUR CODE HERE							
			 ***********************************************/
			// perform a cold-reset of your hardware

			functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("Cold Reset completed.")));
			// leave intermediate state
			functionManager.fireEvent( GEMInputs.SETHALTED );

			logger.debug("coldResettingAction Executed");
		}
	}	
	
	public void fixSoftErrorAction(Object obj) throws UserActionException {
		
		if (obj instanceof StateNotification) {
			
			// triggered by State Notification from child resource
			
			/************************************************
			 * PUT YOUR CODE HERE							
			 ***********************************************/

			return;
		}
		
		else if (obj instanceof StateEnteredEvent) {	
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
	public void runningDegradedAction(Object obj) throws UserActionException {
		if (obj instanceof StateEnteredEvent) {
			functionManager.setDegraded(true);
		}
	}

	//
	// for testing with external inputs  
	//
	// Here we just set our DEGRADED/SOFTERROR state according to an external trigger that sent us to this state.
	// In a real FM, an external event or periodic check will trigger the FM to change state.
	// 
	//
	public void runningSoftErrorDetectedAction(Object obj) throws UserActionException {
		if (obj instanceof StateEnteredEvent) {
			// do not touch degraded 
			functionManager.setSoftErrorDetected(true);
		}
	}

	//
	// for testing with external inputs  
	//
	// Here we just set our DEGRADED/SOFTERROR state according to an external trigger that sent us to this state.
	// In a real FM, an external event or periodic check will trigger the FM to change state.
	// 
	//
	public void runningAction(Object obj) throws UserActionException {
		if (obj instanceof StateEnteredEvent) {
			functionManager.setDegraded(false);
			functionManager.setSoftErrorDetected(false);
		}
	}

	
	@SuppressWarnings("unchecked")
	private void sendCMSError(String errMessage){
		
		// create a new error notification msg
		CMSError error = functionManager.getErrorFactory().getCMSError();
		error.setDateTime(new Date().toString());
		error.setMessage(errMessage);

		// update error msg parameter for GUI
		functionManager.getParameterSet().get(GEMParameters.ERROR_MSG).setValue(new StringT(errMessage));
		
		// send error
		try {
			functionManager.getParentErrorNotifier().sendError(error);
		} catch (Exception e) {
			logger.warn(functionManager.getClass().toString() + ": Failed to send error mesage " + errMessage);
		}
	}
	
	private void cleanUpFMParameters() {
		// Clean-up of the Function Manager parameters
		functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ACTION_MSG,new StringT("")));
		functionManager.getParameterSet().put(new FunctionManagerParameter<StringT>(GEMParameters.ERROR_MSG,new StringT("")));
		functionManager.getParameterSet().put(new FunctionManagerParameter<IntegerT>(GEMParameters.TTS_TEST_FED_ID,new IntegerT(-1)));
	}
	
}
