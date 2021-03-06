
#### Distributed transactions are not supported in .NET Core

Although the .NET Core SQL Server driver supports enlisting in ambient transactions, it does not support participating in distributed transactions so any attempt to use more than one transactional resource within the same ambient transaction will cause an error.

Prior to Version 2.1, the .NET Core SQL Server driver did not support enlisting in ambient transactions and any attempt to do so resulted in the following exception:

```
NServiceBus.Transport.SQLServer.MessagePump|Sql receive operation failed
System.NotSupportedException: Enlisting in Ambient transactions is not supported.
```

To resolve this, choose one of the other transaction modes. Support for `TransactionScope` is expected to be added in future releases of .NET Core.
