package rcms.fm.app.gemfm;

import rcms.fm.fw.EventHandlerException;
import rcms.fm.fw.user.UserActionException;
import rcms.fm.fw.user.UserErrorHandler;
import rcms.statemachine.definition.State;
import rcms.util.logger.RCMSLogger;

/**
 *
 * Error Event Handler class for GEM Level 1 Function Manager.
 *
 * @author Andrea Petrucci, Alexander Oh, Michele Gulmini
 * @maintainer Jose Ruiz, Jared Sturdy
 *
 */

public class GEMErrorHandler extends UserErrorHandler {

    /**
     * <code>m_gemFM</code>: GEMFunctionManager
     */
    GEMFunctionManager m_gemFM = null;

    /**
     * <code>RCMSLogger</code>: RCMS log4j logger.
     */
    static RCMSLogger logger = new RCMSLogger(GEMEventHandler.class);

    public GEMErrorHandler()
        throws EventHandlerException
    {
        // this handler inherits UserErrorHandler
        // so it is already registered for Error events
        String msgPrefix = "[GEM FM] GEMEventHandler::GEMEventHandler(): ";

        // error handler
        addAction(State.ANYSTATE, "errorHandler");

    }

    public void init()
        throws rcms.fm.fw.EventHandlerException
    {
        String msgPrefix = "[GEM FM:: " + ((GEMFunctionManager)getUserFunctionManager()).m_FMname + "] GEMErrorHandler::init(): ";

        logger.info(msgPrefix + "Getting the user function manager");
        m_gemFM = (GEMFunctionManager)getUserFunctionManager();
    }


    public void errorHandler(Object obj)
        throws UserActionException
    {
        String msgPrefix = "[GEM FM:: " + m_gemFM.m_FMname + "] GEMErrorHandler::errorHandler(): ";
        System.out.println(msgPrefix + "Got an event: " + obj.getClass() );
        logger.error(msgPrefix + "Got an event: " + obj.getClass() );
    }
}
