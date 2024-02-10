namespace Simple.Kafka.Extensions;

public static class ConfigurationExtensions
{
    public static IDictionary<string, string> MapConfigs(this IDictionary<string, string> baseConfig,
        IDictionary<string, string> sharedConfig)
    {
        foreach (var config in sharedConfig)
        {
            if (baseConfig.ContainsKey(config.Key)) continue;
            
            baseConfig.Add(config);
        }

        return baseConfig;
    }
}