package com.evolveum.polygon.connector.cloud.objectstorage.csv.s3;

import com.evolveum.polygon.connector.cloud.objectstorage.csv.CloudCsvConfiguration;
import org.identityconnectors.common.logging.Log;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.core.client.builder.SdkSyncClientBuilder;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.utils.AttributeMap;
import software.amazon.awssdk.utils.StringUtils;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;


public class AwsS3ConfigurationBuilder {
    private static final Log LOG = Log.getLog(AwsS3ConfigurationBuilder.class);

    public static AwsSyncClientBuilder prepareClientBuilder(final S3ClientBuilder builder,
                                                            final CloudCsvConfiguration config, boolean isTestMode) throws UnknownHostException, URISyntaxException {
        final AwsS3Configuration awsConfig = prepareConfig(config);

        final AwsCredentialsProvider credentialsProvider = AwsS3ConfigurationBuilder.getCredentials(awsConfig);

        if (!isTestMode) {
            final ProxyConfiguration.Builder proxyConfig = ProxyConfiguration.builder();
            if (StringUtils.isNotBlank(awsConfig.getProxyHost())) {
                proxyConfig.endpoint(new URI(awsConfig.getProxyHost()))
                        .password(awsConfig.getProxyPassword())
                        .username(awsConfig.getProxyUsername());
            }

            final ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder()
                    .proxyConfiguration(proxyConfig.build());

            httpClientBuilder
                    .useIdleConnectionReaper(awsConfig.isUseReaper())
                    .socketTimeout(Duration.ofSeconds(Long.parseLong(awsConfig.getSocketTimeout())))
                    .maxConnections(awsConfig.getMaxConnections())
                    .connectionTimeout(Duration.ofSeconds(Long.parseLong(awsConfig.getConnectionTimeout())))
                    .connectionAcquisitionTimeout(Duration.ofSeconds(Long.parseLong(awsConfig.getClientExecutionTimeout())));

            if (StringUtils.isNotBlank(awsConfig.getLocalAddress())) {
                httpClientBuilder.localAddress(InetAddress.getByName(awsConfig.getLocalAddress()));
            }

            final SdkSyncClientBuilder clientBuilder = builder.httpClientBuilder(httpClientBuilder);
            if (clientBuilder instanceof AwsClientBuilder) {
                final ClientOverrideConfiguration overrideConfig = ClientOverrideConfiguration.builder()
                        .retryPolicy(RetryMode.valueOf(awsConfig.getRetryMode()))
                        .build();
                final AwsClientBuilder awsClientBuilder = (AwsClientBuilder) clientBuilder;
                awsClientBuilder.overrideConfiguration(overrideConfig);
                awsClientBuilder.credentialsProvider(credentialsProvider);

                awsClientBuilder.region(StringUtils.isBlank(awsConfig.getRegion()) ? Region.AWS_GLOBAL : Region.of(awsConfig.getRegion()));

                if (StringUtils.isNotBlank(awsConfig.getEndpoint())) {
                    awsClientBuilder.endpointOverride(new URI(awsConfig.getEndpoint()));
                }
            }

        } else {

            final SdkHttpClient urlClientBuilder = UrlConnectionHttpClient.builder()
                    .buildWithDefaults(AttributeMap.builder()
                            .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, Boolean.TRUE).build());
            builder.httpClient(urlClientBuilder);
            builder.region(StringUtils.isBlank(awsConfig.getRegion()) ? Region.AWS_GLOBAL : Region.of(awsConfig.getRegion()));
            builder.credentialsProvider(credentialsProvider);
            if (StringUtils.isNotBlank(awsConfig.getEndpoint())) {
                builder.endpointOverride(new URI(awsConfig.getEndpoint()));
            }
        }

        return builder;
    }

    private static AwsS3Configuration prepareConfig(final CloudCsvConfiguration config) {
        final AwsS3Configuration props = new AwsS3Configuration();
        props.setRegion(config.getRegion());
        props.setEndpoint(config.getEndpoint());
        props.setCredentialAccessKey(config.getAccessKey());
        props.setCredentialSecretKey(config.getSecretKeyPlain());
        props.setProfileName(config.getProfileName());
        props.setProfilePath(config.getProfilePath());

        return props;
    }

    public static AwsCredentialsProvider getCredentials(final AwsS3Configuration awsConfig) {
        final List chain = new ArrayList<AwsCredentialsProvider>();

        addProviderToChain(nothing -> {
            chain.add(WebIdentityTokenFileCredentialsProvider.create());
            return null;
        });

        chain.add(InstanceProfileCredentialsProvider.create());

        if (StringUtils.isNotBlank(awsConfig.getProfilePath()) && StringUtils.isNotBlank(awsConfig.getProfileName())) {
            addProviderToChain(nothing -> {
                chain.add(ProfileCredentialsProvider.builder()
                        .profileName(awsConfig.getProfileName())
                        .profileFile(ProfileFile.builder().content(Path.of(awsConfig.getProfilePath())).build())
                        .build());
                return null;
            });
        }
        addProviderToChain(nothing -> {
            chain.add(SystemPropertyCredentialsProvider.create());
            return null;
        });

        addProviderToChain(nothing -> {
            chain.add(EnvironmentVariableCredentialsProvider.create());
            return null;
        });

        if (StringUtils.isNotBlank(awsConfig.getCredentialAccessKey()) && StringUtils.isNotBlank(awsConfig.getCredentialSecretKey())) {
            addProviderToChain(nothing -> {
                final AwsBasicCredentials credentials = AwsBasicCredentials.create(awsConfig.getCredentialAccessKey(), awsConfig.getCredentialSecretKey());
                chain.add(StaticCredentialsProvider.create(credentials));
                return null;
            });
        }

        addProviderToChain(nothing -> {
            chain.add(ContainerCredentialsProvider.builder().build());
            return null;
        });

        addProviderToChain(nothing -> {
            chain.add(InstanceProfileCredentialsProvider.builder().build());
            return null;
        });

        return AwsCredentialsProviderChain.builder().credentialsProviders(chain).build();
    }

    private static void addProviderToChain(final Function<Void, Void> func) {
        try {
            func.apply(null);
        } catch (final Exception e) {
            LOG.error(e, null, null);
        }
    }
}
