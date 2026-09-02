using System;

namespace UniGame.StaticEcs.Network
{
    internal static class NetworkTransactionWire
    {
        internal const int CommandHeaderSize = 16;
        internal const int ReceiptSize = 16;
        internal const int MaxPendingTransactions = 256;
        internal const int ReceiptLedgerCapacity = MaxPendingTransactions * 4;

        internal static bool TryWriteCommand(Span<byte> destination,
            NetworkTransactionId transactionId, NetworkTypeId typeId,
            byte version, ReadOnlySpan<byte> command)
        {
            if (transactionId.Value == 0 || typeId.Value == 0 ||
                command.Length > ProtocolLimits.MaxCommandBytes ||
                destination.Length != CommandHeaderSize + command.Length)
                return false;
            Hashing.Write64(destination, 0, transactionId.Value);
            Hashing.Write32(destination, 8, typeId.Value);
            destination[12] = version;
            destination[13] = 0;
            destination[14] = 0;
            destination[15] = 0;
            command.CopyTo(destination.Slice(CommandHeaderSize));
            return true;
        }

        internal static bool TryReadCommand(ReadOnlySpan<byte> source,
            out NetworkTransactionId transactionId, out NetworkTypeId typeId,
            out byte version, out int payloadOffset)
        {
            transactionId = default;
            typeId = default;
            version = 0;
            payloadOffset = 0;
            if (source.Length < CommandHeaderSize || source[13] != 0 ||
                source[14] != 0 || source[15] != 0)
                return false;
            var transactionValue = Hashing.Read64(source, 0);
            var typeValue = Hashing.Read32(source, 8);
            var payloadLength = source.Length - CommandHeaderSize;
            if (transactionValue == 0 || typeValue == 0 ||
                payloadLength > ProtocolLimits.MaxCommandBytes)
                return false;
            transactionId = new NetworkTransactionId(transactionValue);
            typeId = new NetworkTypeId(typeValue);
            version = source[12];
            payloadOffset = CommandHeaderSize;
            return true;
        }

        internal static bool TryWriteReceipt(Span<byte> destination,
            NetworkTransactionId transactionId, NetworkTransactionStatus status,
            uint applicationTick)
        {
            if (destination.Length != ReceiptSize || transactionId.Value == 0 ||
                (byte)status > (byte)NetworkTransactionStatus.SubmissionFailed)
                return false;
            Hashing.Write64(destination, 0, transactionId.Value);
            destination[8] = (byte)status;
            destination[9] = 0;
            destination[10] = 0;
            destination[11] = 0;
            Hashing.Write32(destination, 12, applicationTick);
            return true;
        }

        internal static bool TryReadReceipt(ReadOnlySpan<byte> source,
            out NetworkTransactionId transactionId,
            out NetworkTransactionStatus status, out uint applicationTick)
        {
            transactionId = default;
            status = default;
            applicationTick = 0;
            if (source.Length != ReceiptSize || source[9] != 0 ||
                source[10] != 0 || source[11] != 0)
                return false;
            var transactionValue = Hashing.Read64(source, 0);
            var statusValue = source[8];
            if (transactionValue == 0 ||
                statusValue > (byte)NetworkTransactionStatus.SubmissionFailed)
                return false;
            transactionId = new NetworkTransactionId(transactionValue);
            status = (NetworkTransactionStatus)statusValue;
            applicationTick = Hashing.Read32(source, 12);
            return true;
        }
    }

    /// <summary>Describes one terminal client-side transaction result.</summary>
    public readonly struct NetworkTransactionResult
    {
        internal NetworkTransactionResult(NetworkTransactionId transactionId,
            NetworkTransactionStatus status, uint applicationTick,
            NetworkTypeId typeId)
        {
            TransactionId = transactionId;
            Status = status;
            ApplicationTick = applicationTick;
            TypeId = typeId;
        }

        /// <summary>Gets the transaction identity.</summary>
        public NetworkTransactionId TransactionId { get; }
        /// <summary>Gets the terminal status.</summary>
        public NetworkTransactionStatus Status { get; }
        /// <summary>Gets the server application tick.</summary>
        public uint ApplicationTick { get; }
        /// <summary>Gets the generated command type identifier.</summary>
        public NetworkTypeId TypeId { get; }
    }

    internal sealed class NetworkClientTransaction
    {
        internal NetworkClientTransaction(NetworkTransactionId transactionId,
            object command, NetworkTypeId typeId, in NetworkCommandContext context)
        {
            TransactionId = transactionId;
            Command = command;
            TypeId = typeId;
            Context = context;
        }

        internal NetworkTransactionId TransactionId { get; }
        internal object Command { get; }
        internal NetworkTypeId TypeId { get; }
        internal NetworkCommandContext Context { get; }
    }

    internal readonly struct NetworkClientTransactionResult
    {
        internal NetworkClientTransactionResult(NetworkClientTransaction transaction,
            NetworkTransactionStatus status, uint applicationTick)
        {
            TransactionId = transaction.TransactionId;
            Command = transaction.Command;
            TypeId = transaction.TypeId;
            Context = new NetworkCommandContext(transaction.Context.PeerId,
                transaction.Context.Epoch, transaction.Context.Sequence,
                applicationTick, NetworkCommandDelivery.Transaction,
                transaction.TypeId, transaction.TransactionId);
            Status = status;
            ApplicationTick = applicationTick;
        }

        internal NetworkTransactionId TransactionId { get; }
        internal object Command { get; }
        internal NetworkTypeId TypeId { get; }
        internal NetworkCommandContext Context { get; }
        internal NetworkTransactionStatus Status { get; }
        internal uint ApplicationTick { get; }
    }

    internal sealed class NetworkServerTransaction
    {
        internal NetworkServerTransaction(NetworkTransactionId transactionId,
            NetworkCommandEnvelope envelope, NetworkSchemaEntry entry,
            uint applicationTick)
        {
            TransactionId = transactionId;
            Envelope = envelope;
            Entry = entry;
            ApplicationTick = applicationTick;
        }

        internal NetworkTransactionId TransactionId { get; }
        internal NetworkCommandEnvelope Envelope;
        internal NetworkSchemaEntry Entry { get; }
        internal uint ApplicationTick { get; }
        internal bool Dispatched { get; set; }
        internal NetworkTransactionStatus? CompletionStatus { get; set; }
        internal bool ReceiptSent { get; set; }

        internal void Dispose() => Envelope.Dispose();
    }

    internal readonly struct NetworkServerTransactionReceipt
    {
        internal NetworkServerTransactionReceipt(NetworkTransactionId transactionId,
            NetworkTransactionStatus status, uint applicationTick)
        {
            TransactionId = transactionId;
            Status = status;
            ApplicationTick = applicationTick;
        }

        internal NetworkTransactionId TransactionId { get; }
        internal NetworkTransactionStatus Status { get; }
        internal uint ApplicationTick { get; }
    }
}
