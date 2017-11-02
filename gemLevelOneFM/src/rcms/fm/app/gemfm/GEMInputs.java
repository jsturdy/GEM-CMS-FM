package rcms.fm.app.gemfm;

import rcms.statemachine.definition.Input;

/**
 * Definition of GEM Level 1 Function Manager Commands
 *
 * @author Andrea Petrucci, Alexander Oh, Michele Gulmini
 * @maintainer Jose Ruiz, Jared Sturdy
 */

public class GEMInputs {

    // Defined commands for the level 1 Function Manager

    public static final Input INITIALIZE = new Input("Initialize");
    public static final Input CONFIGURE  = new Input("Configure");
    public static final Input START      = new Input("Start");
    public static final Input ENABLE     = new Input("Enable");
    public static final Input STOP       = new Input("Stop");
    public static final Input HALT       = new Input("Halt");
    public static final Input PAUSE      = new Input("Pause");
    public static final Input RESUME     = new Input("Resume");
    public static final Input RECOVER    = new Input("Recover");
    public static final Input RESET      = new Input("Reset");
    public static final Input COLDRESET  = new Input("ColdReset");

    public static final Input SETCONFIGURE = new Input("SetConfigure");
    public static final Input SETSTART     = new Input("SetStart");
    public static final Input SETHALT      = new Input("SetHalt");
    public static final Input SETPAUSE     = new Input("SetPause");
    public static final Input SETRESUME    = new Input("SetResume");
    public static final Input SETRESET     = new Input("SetReset");

    public static final Input SETCONFIGURED = new Input("SetConfigured");
    public static final Input SETRUNNING    = new Input("SetRunning");
    public static final Input SETHALTED     = new Input("SetHalted");
    public static final Input SETPAUSED     = new Input("SetPaused");
    public static final Input SETRESUMED    = new Input("SetResumed");

    public static final Input SETRUNNINGDEGRADED          = new Input("SetRunningDegraded");
    public static final Input SETRUNNINGSOFTERRORDETECTED = new Input("SetRunningSoftErrorDetected");
    public static final Input SETRESUMEDDEGRADED          = new Input("SetResumedDegraded");
    public static final Input SETRESUMEDSOFTERRORDETECTED = new Input("SetResumedSoftErrorDectected");

    public static final Input FIXSOFTERROR         = new Input("FixSoftError");
    public static final Input PREPARE_TTSTEST_MODE = new Input("PrepareTTSTestMode");
    public static final Input SETTTSTEST_MODE      = new Input("SetTTSTestMode");
    public static final Input TEST_TTS             = new Input("TestTTS");

    // Go to error
    public static final Input SETERROR = new Input("SetError");

}
