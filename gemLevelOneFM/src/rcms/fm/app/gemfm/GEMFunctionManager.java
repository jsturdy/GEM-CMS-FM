package rcms.fm.app.gemfm;


import rcms.fm.fw.parameter.CommandParameter;
import rcms.fm.fw.parameter.ParameterSet;
import rcms.fm.fw.user.UserActionException;
import rcms.fm.fw.user.UserFunctionManager;
import rcms.fm.resource.QualifiedResourceContainer;
import rcms.fm.resource.qualifiedresource.XdaqApplicationContainer;
import rcms.statemachine.definition.State;
import rcms.statemachine.definition.StateMachineDefinitionException;
import rcms.util.logger.RCMSLogger;

import rcms.resourceservice.db.resource.fm.FunctionManagerResource;

import rcms.util.logsession.LogSessionException;
import rcms.util.logsession.LogSessionConnector;

import rcms.utilities.runinfo.RunInfo;

/**
 * Example of Function Machine for controlling an Level 1 Function Manager.
 * 
 * @author Andrea Petrucci, Alexander Oh, Michele Gulmini
 * @maintainer Jose Ruiz
 */
public class GEMFunctionManager extends UserFunctionManager {

	/**
	 * <code>RCMSLogger</code>: RCMS log4j Logger
	 */
	static RCMSLogger logger = new RCMSLogger(GEMFunctionManager.class);

	/**
	 * define some containers
	 */
	public XdaqApplicationContainer containerXdaqApplication = null;

	/**
	 * define specific application containers
	 */
	public XdaqApplicationContainer cEVM = null;

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
	 * <code>calcState</code>: Calculated State.
	 */
	public State calcState = null;

	// In the template FM we store whether we are degraded in a boolean
	boolean degraded = false;         

	// In the template FM we store whether we have detected a softError in a boolean
	boolean softErrorDetected=false;

        // connector to the RunInfo database
        public RunInfo GEMRunInfo = null;    

        // set from the controlled EventHandler
        public String  RunType = "";
        public Integer RunNumber = 0;
        //public Integer CachedRunNumber = 0;

        public String FMname = "empty";
	
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
	public void createAction(ParameterSet<CommandParameter> pars) throws UserActionException {
		//
		// This method is called by the framework when the Function Manager is
		// created.

		System.out.println("createAction called.");
		logger.debug("createAction called.");

		// Retrieve the configuration for this Function Manager from the Group
		FunctionManagerResource fmConf = ((FunctionManagerResource) qualifiedGroup.getGroup().getThisResource());

		FMname = fmConf.getName();

		System.out.println("createAction executed.");
		logger.debug("createAction executed.");

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see rcms.statemachine.user.UserStateMachine#destroyAction()
	 */
	public void destroyAction() throws UserActionException {
		//
		// This method is called by the framework when the Function Manager is
		// destroyed.
		//
	    qualifiedGroup.destroy();
	    
		System.out.println("destroyAction called");
		logger.debug("destroyAction called");

		// try to close any open session ID only if we are in local run mode i.e. not CDAQ and not miniDAQ runs and if it's a LV1FM 
		//if (RunType.equals("local") && !containerFMChildren.isEmpty()) { closeSessionId(); }
		closeSessionId(); //NEEDS TO BE CORRECTED TO ONLY BE CALLED IN LOCAL RUNS

		System.out.println("destroyAction executed");
		logger.debug("destroyAction executed");
	}

	/**
	 * add parameters to parameterSet. After this they are accessible.
	 */
	private void addParameters() {

		// add parameters to parameter Set so they are visible.
		parameterSet = GEMParameters.LVL_ONE_PARAMETER_SET;

	}

	public void init() throws StateMachineDefinitionException,
			rcms.fm.fw.EventHandlerException {

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

		// get log session connector
		logSessionConnector = getLogSessionConnector();

		// get session ID
		getSessionId();

	}

    // get a session Id
    protected void getSessionId() {
      String user = getQualifiedGroup().getGroup().getDirectory().getUser();
      String description = getQualifiedGroup().getGroup().getDirectory().getFullPath();
      int sessionId = 0;

      logger.debug("[GEM base] Log session connector: " + logSessionConnector );

      if (logSessionConnector != null) {
        try {
          sessionId = logSessionConnector.createSession( user, description );
          logger.debug("[GEM base] New session Id obtained =" + sessionId );
        }
        catch (LogSessionException e1) {
          logger.warn("[GEM base] Could not get session ID, using default = " + sessionId + ". Exception: ",e1);
        }
      }
      else {
        logger.warn("[GEM base] logSessionConnector = " + logSessionConnector + ", using default = " + sessionId + ".");
      }

      // put the session ID into parameter set             
      getParameterSet().get("SID").setValue(new IntegerT(sessionId));
    }

  // close session Id. This routine is called always when functionmanager gets destroyed.
  protected void closeSessionId() {
    if (logSessionConnector != null) {
      int sessionId = 0;
      try {
        sessionId = ((IntegerT)getParameterSet().get("SID").getValue()).getInteger();
      }
      catch (Exception e) {
        logger.warn("[GEM " + FMname + "] Could not get sessionId for closing session.\nNot closing session.\nThis is OK if no sessionId was requested from within GEM land, i.e. global runs.",e);
      }
      try {
        logger.debug("[GEM " + FMname + "] Trying to close log sessionId = " + sessionId );
        logSessionConnector.closeSession(sessionId);
        logger.debug("[GEM " + FMname + "] ... closed log sessionId = " + sessionId );
      }
      catch (LogSessionException e1) {
        logger.warn("[GEM " + FMname + "] Could not close sessionId, but sessionId was requested and used.\nThis is OK only for global runs.\nException: ",e1);
      }
    }

  }
	
	public boolean isDegraded() {
		// FM may check whether it is currently degraded if such functionality exists
		return degraded;
	}
	
	public boolean hasSoftError() {
		// FM may check whether the system has a soft error if such functionality exists
		return softErrorDetected;
	}
	
	// only needed if FM cannot check for degradation
	public void setDegraded(boolean degraded) {
		this.degraded = degraded;
	}
	
	// only needed if FM cannot check for softError
	public void setSoftErrorDetected(boolean softErrorDetected) {
		this.softErrorDetected = softErrorDetected;
	}
}
