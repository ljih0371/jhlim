namespace mcactmodule
{
    class YamlReader
    {
        public class YamlConfig
        {
            public YamlLine line {get; set;}
            public YamlServer server {get; set;}
            public YamlParam param {get; set;}
        }
        public class YamlLine
        {
            public string name {get; set;}
            public string plc {get; set;}
        }
        public class YamlServer
        {
            public string ip {get; set;}
            public string uid {get; set;}
            public string pwd {get; set;}
            public string dbname {get; set;}
            public YamlTBName tbname {get; set;}
        }
        public class YamlTBName
        {
            public string report {get; set;}
            public string time {get;set;}
        }
        public class YamlParam
        {
            public string cutoff {get; set;}
            public YamlRuleModel rule_model {get; set;}
            public YamlMlModel ml_model {get; set;}
        }

        public class YamlRuleModel
        {
            public string FTUR_ENRG {get; set;}
            public string FTUR_WVFM_STDDEV{get; set;}
            public string FTUR_TRGER{get; set;}
        }

        public class YamlMlModel
        {
            public YamlFilterActiveNoiseCanceling FilterActiveNoiseCanceling {get; set;}
        }

        public class YamlFilterActiveNoiseCanceling
        {
            public string prop_decrease {get; set;}
        }
    }
}