namespace MC_CheckingModule
{
    using System;
    using Newtonsoft.Json;

    public class MessageBody
    {
        [JsonProperty("path")]
        public string Path {get; set;} 
        [JsonProperty("chtime")]
        public DateTime ChTime {get; set;}
    }
}