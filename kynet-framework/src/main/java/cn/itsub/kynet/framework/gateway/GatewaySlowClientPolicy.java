package cn.itsub.kynet.framework.gateway;

public enum GatewaySlowClientPolicy {
    /** Buffer temporarily, but still close when pending limits or the unwritable timeout are exceeded. */
    BUFFER,
    /** Drop new outbound packets while the channel is not writable, then close on sustained timeout. */
    DROP,
    /** Close the connection as soon as the channel becomes not writable. */
    CLOSE
}
