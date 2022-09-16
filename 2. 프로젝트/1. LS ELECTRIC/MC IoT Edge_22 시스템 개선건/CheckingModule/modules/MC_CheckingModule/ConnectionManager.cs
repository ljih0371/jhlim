namespace MC_CheckingModule
{
    using System;
    using System.IO;
    using System.Runtime.InteropServices;
    using System.Runtime.Loader;
    using System.Security.Cryptography.X509Certificates;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client;
    using System.Collections.Generic;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt;
    using Microsoft.Azure.Devices.Shared;
    using Newtonsoft.Json;
    using System.Net;
    using System.Diagnostics;

    public class ConnectionManager
    {
        /// <summary>
        /// Handles cleanup operations when app is cancelled or unloads
        /// </summary>
        public static Task WhenCancelled(CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);
            return tcs.Task;
        }

        /// <summary>
        /// Initializes the ModuleClient and sets up the callback to receive
        /// messages containing temperature information
        /// </summary>

        public static async Task<ModuleClient> Init()
        {
            //MQTT 통신
            //MqttTransportSettings mqttSetting = new MqttTransportSettings(Microsoft.Azure.Devices.Client.TransportType.Mqtt_Tcp_Only);
            //ITransportSettings[] settings = { mqttSetting };

            // AMQP 통신(11.08) - 안정화 시점에 테스트 예정
            AmqpTransportSettings amqpSetting = new AmqpTransportSettings(Microsoft.Azure.Devices.Client.TransportType.Amqp_Tcp_Only);
            ITransportSettings[] settings = { amqpSetting };

            // Open a connection to the Edge runtime
            ModuleClient ioTHubModuleClient = await ModuleClient.CreateFromEnvironmentAsync(settings);
            await ioTHubModuleClient.OpenAsync();
            //LogManager.LogWrite("IoT Hub module client initialized.", LogManager.MessageStatus.Usual);
            Console.WriteLine("IoT Hub module client initialized.");

            var moduleTwin = await ioTHubModuleClient.GetTwinAsync();
            var moduleTwinCollection = moduleTwin.Properties.Desired;
            // as this runs in a loop we don't await
            return ioTHubModuleClient;
        }

        //Send message
        public static async Task SendData(ModuleClient moduleClient, string messageToSend)
        {
            try
            {
                var messageString = messageToSend;

                if (messageString != string.Empty)
                {
                    //var logstring = "@@@@@@@@@" + messageString + "";
                    //Console.WriteLine(logstring);
                    var messageBytes = Encoding.UTF8.GetBytes(messageString);
                    var message = new Message(messageBytes);
                    message.ContentEncoding = "utf-8";
                    message.ContentType = "application/json";

                    await moduleClient.SendEventAsync("messageOutput", message);
                    Console.WriteLine("message sent");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] - ConnectionManager.SendData() - Unexpected Exception : {ex.Message}");
                Console.WriteLine($"\t{ex.ToString()}");
                throw;
            }
        }

    }
}