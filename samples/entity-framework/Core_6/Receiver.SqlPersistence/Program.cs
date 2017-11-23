using System;
using System.Data.SqlClient;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.Persistence.Sql;
using NServiceBus.Transport.SQLServer;

class Program
{
    static async Task Main()
    {
        var connection = @"Data Source=.\SqlExpress;Database=NsbSamplesEfUow;Integrated Security=True";
        Console.Title = "Samples.EntityFrameworkUnitOfWork.Receiver";
        using (var receiverDataContext = new ReceiverDataContext(new SqlConnection(connection)))
        {
            receiverDataContext.Database.Initialize(true);
        }

        var endpointConfiguration = new EndpointConfiguration("Samples.EntityFrameworkUnitOfWork.Receiver");
        endpointConfiguration.EnableInstallers();
        endpointConfiguration.SendFailedMessagesTo("error");

        var transport = endpointConfiguration.UseTransport<SqlServerTransport>();
        transport.ConnectionString(connection);
        var routing = transport.Routing();
        routing.RouteToEndpoint(typeof(OrderAccepted).Assembly, "Samples.EntityFrameworkUnitOfWork.Sender");
        routing.RegisterPublisher(typeof(OrderAccepted).Assembly, "Samples.EntityFrameworkUnitOfWork.Sender");

        transport.DefaultSchema("receiver");

        transport.UseSchemaForEndpoint("Samples.EntityFrameworkUnitOfWork.Sender", "sender");
        transport.UseSchemaForQueue("error", "dbo");
        transport.UseSchemaForQueue("audit", "dbo");

        var persistence = endpointConfiguration.UsePersistence<SqlPersistence>();
        persistence.ConnectionBuilder(() => new SqlConnection(connection));
        persistence.SubscriptionSettings().DisableCache();
        var dialect = persistence.SqlDialect<SqlDialect.MsSqlServer>();
        dialect.Schema("receiver");

        #region UnitOfWork_SQL

        var pipeline = endpointConfiguration.Pipeline;
        pipeline.Register(new UnitOfWorkSetupBehaviorBehavior(storageSession =>
        {
            var dbConnection = storageSession.SqlPersistenceSession().Connection;
            var context = new ReceiverDataContext(dbConnection);

            //Use the same underlying ADO.NET transaction
            context.Database.UseTransaction(storageSession.SqlPersistenceSession().Transaction);

            //Call SaveChanges before completing storage session
            storageSession.SqlPersistenceSession().OnSaveChanges(x => context.SaveChangesAsync());

            return context;
        }), "Sets up unit of work for the message");

        #endregion

        SqlHelper.CreateSchema(connection, "receiver");
        var endpointInstance = await Endpoint.Start(endpointConfiguration)
            .ConfigureAwait(false);

        try
        {
            Console.WriteLine("Press any key to exit");
            Console.ReadKey();
        }
        finally
        {
            await endpointInstance.Stop()
                .ConfigureAwait(false);
        }
    }
}