package com.evolveum.polygon.connector.cloud.objectstorage.csv.s3;


public class AwsS3Configuration {

    //RequiredProperty
    private String credentialAccessKey;

    //RequiredProperty
    private String credentialSecretKey;

    //RequiredProperty
    private String region;

    //Possible RequiredProperty
    private String endpoint;

    private String profileName;

    private String profilePath;

    private int maxConnections = 10;

    private String connectionTimeout = "5000";

    private String socketTimeout = "5000";

    private String clientExecutionTimeout = "10000";

    private boolean useReaper;

    private String proxyHost;

    private String proxyPassword;

    private String proxyUsername;

    private String retryMode = "STANDARD";

    private String localAddress;


    public String getCredentialAccessKey() {
        return credentialAccessKey;
    }

    public void setCredentialAccessKey(String credentialAccessKey) {
        this.credentialAccessKey = credentialAccessKey;
    }

    /**
     * Use secret key provided by AWS to authenticate.
     */
    public String getCredentialSecretKey() {
        return credentialSecretKey;
    }

    public void setCredentialSecretKey(String credentialSecretKey) {
        this.credentialSecretKey = credentialSecretKey;
    }

    /**
     * AWS region used.
     */
    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    /**
     * AWS custom endpoint.
     */
    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * Profile name to use.
     */
    public String getProfileName() {
        return profileName;
    }

    public void setProfileName(String profileName) {
        this.profileName = profileName;
    }

    /**
     * Profile path.
     */
    public String getProfilePath() {
        return profilePath;
    }

    public void setProfilePath(String profilePath) {
        this.profilePath = profilePath;
    }

    /**
     * Maximum connections setting.
     */
    public int getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    /**
     * Connection timeout.
     */
    public String getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(String connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    /**
     * Socket timeout.
     */
    public String getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(String socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    /**
     * Client execution timeout.
     */
    public String getClientExecutionTimeout() {
        return clientExecutionTimeout;
    }

    public void setClientExecutionTimeout(String clientExecutionTimeout) {
        this.clientExecutionTimeout = clientExecutionTimeout;
    }

    /**
     * Flag that indicates whether to use reaper.
     */
    public boolean isUseReaper() {
        return useReaper;
    }

    public void setUseReaper(boolean useReaper) {
        this.useReaper = useReaper;
    }

    /**
     *  Optionally specifies the proxy host to connect through.
     */
    public String getProxyHost() {
        return proxyHost;
    }

    public void setProxyHost(String proxyHost) {
        this.proxyHost = proxyHost;
    }

    /**
     *  Optionally specifies the proxy password to connect through.
     */
    public String getProxyPassword() {
        return proxyPassword;
    }

    public void setProxyPassword(String proxyPassword) {
        this.proxyPassword = proxyPassword;
    }

    /**
     *  Optionally specifies the proxy username to connect through.
     */
    public String getProxyUsername() {
        return proxyUsername;
    }

    public void setProxyUsername(String proxyUsername) {
        this.proxyUsername = proxyUsername;
    }

    /**
     * Outline the requested retry mode.
     * Accepted values are {@code STANDARD, LEGACY}.
     */
    public String getRetryMode() {
        return retryMode;
    }

    public void setRetryMode(String retryMode) {
        this.retryMode = retryMode;
    }

    public String getLocalAddress() {
        return localAddress;
    }

    public void setLocalAddress(String localAddress) {
        this.localAddress = localAddress;
    }
}
