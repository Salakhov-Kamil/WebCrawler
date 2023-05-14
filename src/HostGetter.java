import java.net.MalformedURLException;

/**
 * Gives the host by url
 *
 * @author Salakhov Kamil
 */
public interface HostGetter {
    String getHost(String url) throws MalformedURLException;
}
