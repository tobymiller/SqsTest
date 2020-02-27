using Amazon.Lambda.Core;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.SQS;
using Amazon.XRay.Recorder.Handlers.AwsSdk;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

[assembly:LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace AwsDotnetCsharp
{
    public class Handler
    {
        private readonly IServiceProvider _sc;

        public static async Task Main(string[] args)
        {
            await new Handler().Run();
        }

        public Handler()
        {
            Environment.SetEnvironmentVariable("AWS_XRAY_CONTEXT_MISSING", "LOG_ERROR");
            AWSSDKHandler.RegisterXRayForAllServices();
            AWSConfigs.LoggingConfig.LogTo = LoggingOptions.Console;
            var config = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .Build();
            var services = new ServiceCollection();
            services.AddSingleton<IConfiguration>(config);
            services.AddAWSService<IAmazonSQS>();
            services.AddHttpClient();
            _sc = services.BuildServiceProvider();
        }

        public async Task<string> Run()
        {
            var startTime = DateTime.Now;
            var client = _sc.GetService<IAmazonSQS>();
            var sqsUrl = Environment.GetEnvironmentVariable("sqs_url");
            var sw = new Stopwatch();
            var message = string.Join(", ", Enumerable.Range(0, 100).Select(n => "repeating text"));
            var innerLoopCount = 1024;
            var timesBag = new ConcurrentBag<long>();
            var semaphore = new SemaphoreSlim(512);
            Func<Task> task = async () =>
            {
                await Task.WhenAll(Enumerable.Range(0, innerLoopCount).ToArray()
                    .Select(async n =>
                    {
                        await Task.Yield();
                        await semaphore.WaitAsync();
                        try
                        {
                            var sw2 = new Stopwatch();
                            sw2.Start();
                            try
                            {
                                var sendMessageAsync = client.SendMessageAsync(sqsUrl, message, CancellationToken.None);
                                await sendMessageAsync;
                            }
                            catch (Exception)
                            {
                                // we still track this one as it threw a legit exception (throttling probably)
                            }

                            sw2.Stop();
                            if (sw2.ElapsedMilliseconds < 90000)
                            {
                                timesBag.Add(sw2.ElapsedMilliseconds);
                            }
                            else
                            {
                                Console.WriteLine("Took 100 s!");
                            }
                        }
                        finally
                        {
                            semaphore.Release();
                        }
                    }));
            };
            sw.Start();
            await await Task.WhenAny(task(), Task.Delay(TimeSpan.FromMinutes(2)));
            sw.Stop();

            var maxTime = timesBag.Max();
            var timedOut = sw.ElapsedMilliseconds > 90000 ? "Timed out!" : "";
            var averageTime = timesBag.Average();
            var output = $"{timedOut}Started at {startTime}. Completed {timesBag.Count} of {innerLoopCount} sqs calls in {sw.ElapsedMilliseconds} ms. Longest time was {maxTime} ms, and average {averageTime} ms";
            Console.WriteLine(output);
            return output;
        }
    }
}