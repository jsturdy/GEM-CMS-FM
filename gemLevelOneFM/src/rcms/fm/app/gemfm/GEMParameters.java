package rcms.fm.app.gemfm;

import rcms.fm.fw.parameter.CommandParameter;
import rcms.fm.fw.parameter.FunctionManagerParameter;
import rcms.fm.fw.parameter.ParameterSet;
import rcms.fm.fw.parameter.CommandParameter.Required;
import rcms.fm.fw.parameter.FunctionManagerParameter.Exported;
import rcms.fm.fw.parameter.type.DoubleT;
import rcms.fm.fw.parameter.type.IntegerT;
import rcms.fm.fw.parameter.type.StringT;

/**
 * Defined GEM Level 1 Function Manager parameters.
 *
 * Standard parameter definitions for Level 1 Function Manager
 *
 * STATE					: State name the function manager is currently in
 *
 * ACTION_MSG 				: Short description of current activity, if any
 * ERROR_MSG 				: In case of an error contains a description of the error
 * COMPLETION 				: Completion of an activity can be signaled through this numerical value 0 < PROGRESS_BAR < 1
 *
 * For more details => https://twiki.cern.ch/twiki/bin/view/CMS/StdFMParameters
 *
 * @author Andrea Petrucci, Alexander Oh, Michele Gulmini, Hannes Sakulin
 * @maintainer Jose Ruiz, Jared Sturdy
 *
 */

public class GEMParameters {

    /**
     * standard parameter definitions for Level 1 Function Manager
     */

    public static final String STATE      = "STATE";
    public static final String ACTION_MSG = "ACTION_MSG";
    public static final String ERROR_MSG  = "ERROR_MSG";
    public static final String COMPLETION = "COMPLETION";

    // To be exported after initialize
    public static final String INITIALIZED_WITH_SID             = "INITIALIZED_WITH_SID";
    public static final String INITIALIZED_WITH_GLOBAL_CONF_KEY = "INITIALIZED_WITH_GLOBAL_CONF_KEY";
    // public static final String INITIALIZED_WITH_RUN_NUMBER      = "INITIALIZED_WITH_RUN_NUMBER";

    // To be exported after configure
    public static final String CONFIGURED_WITH_FED_ENABLE_MASK = "CONFIGURED_WITH_FED_ENABLE_MASK";
    public static final String CONFIGURED_WITH_RUN_KEY         = "CONFIGURED_WITH_RUN_KEY";
    public static final String CONFIGURED_WITH_RUN_NUMBER      = "CONFIGURED_WITH_RUN_NUMBER";

    // To be exported after start
    public static final String STARTED_WITH_RUN_NUMBER = "STARTED_WITH_RUN_NUMBER";

    // Command parameters
    public static final String SID             = "SID";
    public static final String GLOBAL_CONF_KEY = "GLOBAL_CONF_KEY";

    public static final String RUN_NUMBER     = "RUN_NUMBER";
    public static final String RUN_KEY        = "RUN_KEY";
    public static final String RUN_SEQ_NUMBER = "RUN_SEQ_NUMBER";

    public static final String FED_ENABLE_MASK         = "FED_ENABLE_MASK";
    public static final String TRIGGER_NUMBER_AT_PAUSE = "TRIGGER_NUMBER_AT_PAUSE";

    // Command parameters for TTS testing
    public static final String TTS_TEST_FED_ID          = "TTS_TEST_FED_ID";
    public static final String TTS_TEST_MODE            = "TTS_TEST_MODE";
    public static final String TTS_TEST_PATTERN         = "TTS_TEST_PATTERN";
    public static final String TTS_TEST_SEQUENCE_REPEAT = "TTS_TEST_SEQUENCE_REPEAT";

    // Command parameters for TCDS
    public static final String LPM_HW_CFG = "LPM_HW_CFG";
    public static final String ICI_HW_CFG = "ICI_HW_CFG";
    public static final String PI_HW_CFG  = "PI_HW_CFG";

    // standard level 1 parameter set
    public static final ParameterSet<FunctionManagerParameter> LVL_ONE_PARAMETER_SET = new ParameterSet<FunctionManagerParameter>();

    // level 1 TCDS parameter set
    public static final ParameterSet<CommandParameter> TCDS_PARAMETER_SET = new ParameterSet<CommandParameter>();


    static {

        /**
         * State of the Function Manager is currently in
         */
        LVL_ONE_PARAMETER_SET.put(new FunctionManagerParameter<StringT>(STATE, new StringT("Created: GEMINI says hello world!"),
                                                                        Exported.READONLY));

        /**
         * parameters for monitoring
         */
        LVL_ONE_PARAMETER_SET.put(new FunctionManagerParameter<StringT>(ACTION_MSG, new StringT(""),
                                                                        Exported.READONLY));
        LVL_ONE_PARAMETER_SET.put(new FunctionManagerParameter<StringT>(ERROR_MSG, new StringT(""),
                                                                        Exported.READONLY));
        LVL_ONE_PARAMETER_SET.put(new FunctionManagerParameter<DoubleT>(COMPLETION, new DoubleT(-1),
                                                                        Exported.READONLY));

        /**
         * Session Identifier
         */
        // Database connection session identifier
        LVL_ONE_PARAMETER_SET.put(new FunctionManagerParameter<IntegerT> (SID, new IntegerT(0),
                                                                          Exported.READONLY) );
        LVL_ONE_PARAMETER_SET.put(new FunctionManagerParameter<StringT>(GLOBAL_CONF_KEY, new StringT("not set"),
                                                                        Exported.READONLY) );
        LVL_ONE_PARAMETER_SET.put(new FunctionManagerParameter<IntegerT>(RUN_NUMBER, new IntegerT(-1),
                                                                         Exported.READONLY) );
        LVL_ONE_PARAMETER_SET.put(new FunctionManagerParameter<StringT>(RUN_KEY, new StringT("not set"),
                                                                        Exported.READONLY) );
        LVL_ONE_PARAMETER_SET.put(new FunctionManagerParameter<IntegerT>(RUN_SEQ_NUMBER, new IntegerT(-1),
                                                                         Exported.READONLY) );

        // Configuration information for l0:  SID on initialize
        LVL_ONE_PARAMETER_SET.put(new FunctionManagerParameter<StringT>(INITIALIZED_WITH_SID, new StringT("not set"),
                                                                        Exported.READONLY) );
        LVL_ONE_PARAMETER_SET.put(new FunctionManagerParameter<StringT>(INITIALIZED_WITH_GLOBAL_CONF_KEY, new StringT("not set"),
                                                                        Exported.READONLY) );

        LVL_ONE_PARAMETER_SET.put(new FunctionManagerParameter<StringT>(CONFIGURED_WITH_FED_ENABLE_MASK, new StringT("not set"),
                                                                        Exported.READONLY) );
        LVL_ONE_PARAMETER_SET.put(new FunctionManagerParameter<StringT>(CONFIGURED_WITH_RUN_KEY, new StringT("not set"),
                                                                        Exported.READONLY) );
        LVL_ONE_PARAMETER_SET.put(new FunctionManagerParameter<IntegerT>(CONFIGURED_WITH_RUN_NUMBER, new IntegerT(-1),
                                                                         Exported.READONLY) );

        LVL_ONE_PARAMETER_SET.put(new FunctionManagerParameter<IntegerT>(STARTED_WITH_RUN_NUMBER, new IntegerT(-1),
                                                                         Exported.READONLY) );
    }

    static {

        /**
         * LPM hardware configuration to be sent to the LPMController
         */
        TCDS_PARAMETER_SET.put(new CommandParameter<StringT>(LPM_HW_CFG, new StringT(""), Required.NO));

        /**
         * ICI hardware configuration to be sent to the ICIController
         */
        TCDS_PARAMETER_SET.put(new CommandParameter<StringT>(ICI_HW_CFG, new StringT(""), Required.NO));

        /**
         * PI hardware configuration to be sent to the PIController
         */
        TCDS_PARAMETER_SET.put(new CommandParameter<StringT>(PI_HW_CFG,  new StringT(""), Required.NO));
    }

}
