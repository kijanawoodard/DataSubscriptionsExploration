using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace RavenDataSubscriptions
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var store = new DocumentStore {ConnectionStringName = "RavenDB"}.Initialize())
            {
                CreateDocuments(store);

                var id = store.Subscriptions.Create(new SubscriptionCriteria<Order>
                {
                    KeyStartsWith = "orders/",                    
                    PropertiesNotMatch = new Dictionary<Expression<Func<Order, object>>, RavenJToken>()
                    {
                        { x => x.Customer, "Fred Doe"}
                    }
                });

                var orders = store.Subscriptions.Open<Order>(id, new SubscriptionConnectionOptions()
                {
                    BatchOptions = new SubscriptionBatchOptions()
                    {
                        MaxDocCount = 16 * 1024,
                        MaxSize = 4 * 1024 * 1024,
                        AcknowledgmentTimeout = TimeSpan.FromMinutes(3)
                    },
                    IgnoreSubscribersErrors = false,
                    ClientAliveNotificationInterval = TimeSpan.FromSeconds(30)
                });

                orders.Subscribe(x =>
                {
                    SendOrderReceivedEmail(x);
                    Console.WriteLine($"Processed {x.Id} {x.Customer}");
                });

                Console.ReadLine();
                store.DatabaseCommands.GlobalAdmin.DeleteDatabase(databaseName: "SubscriptionsTest", hardDelete: true);
            }
        }

        private static void SendOrderReceivedEmail(Order order)
        {
            //send email
        }

        private static void CreateDocuments(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Order
                {
                    Id = "orders/1",
                    Customer = "John Doe",
                    Amount = 10.01m
                });

                session.Store(new Order
                {
                    Id = "orders/2",
                    Customer = "Jane Doe",
                    Amount = 10.02m
                });

                session.Store(new Order
                {
                    Id = "orders/3",
                    Customer = "Fred Doe",
                    Amount = 10.03m
                });

                session.SaveChanges();
            }
        }
    }

    class Order
    {
        public string Id { get; set; }
        public string Customer { get; set; }
        public decimal Amount { get; set; }
        public bool Processed { get; set; }
    }
}
