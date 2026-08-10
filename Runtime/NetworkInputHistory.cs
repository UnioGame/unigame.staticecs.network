namespace UniGame.StaticEcs.Network
{
    using System;

    /// <summary>Stores one typed input together with its client sequence and target tick.</summary>
    public readonly struct NetworkInputFrame<TInput>
        where TInput : struct, INetworkInput
    {
        /// <summary>Creates an immutable input frame.</summary>
        public NetworkInputFrame(uint tick, uint sequence, in TInput input)
        {
            Tick = tick;
            Sequence = sequence;
            Input = input;
        }

        /// <summary>Gets the target authoritative tick.</summary>
        public uint Tick { get; }

        /// <summary>Gets the monotonic client input sequence.</summary>
        public uint Sequence { get; }

        /// <summary>Gets the typed input value.</summary>
        public TInput Input { get; }
    }

    /// <summary>Provides a bounded allocation-free tick-indexed state history.</summary>
    public sealed class PredictionHistory<TState>
        where TState : struct
    {
        private readonly uint[] _ticks;
        private readonly TState[] _states;
        private readonly bool[] _occupied;

        /// <summary>Creates a history with fixed tick capacity.</summary>
        public PredictionHistory(int capacity)
        {
            if (capacity <= 0) throw new ArgumentOutOfRangeException(nameof(capacity));
            _ticks = new uint[capacity];
            _states = new TState[capacity];
            _occupied = new bool[capacity];
        }

        /// <summary>Gets the maximum number of retained ticks.</summary>
        public int Capacity => _states.Length;

        /// <summary>Stores or replaces one tick value.</summary>
        public void Store(uint tick, in TState state)
        {
            int index = (int)(tick % (uint)_states.Length);
            _ticks[index] = tick;
            _states[index] = state;
            _occupied[index] = true;
        }

        /// <summary>Gets one value when its exact tick remains retained.</summary>
        public bool TryGet(uint tick, out TState state)
        {
            int index = (int)(tick % (uint)_states.Length);
            if (_occupied[index] && _ticks[index] == tick)
            {
                state = _states[index];
                return true;
            }

            state = default;
            return false;
        }

        /// <summary>Removes all retained values through the supplied tick.</summary>
        public void DiscardThrough(uint tick)
        {
            for (var i = 0; i < _occupied.Length; i++)
            {
                if (_occupied[i] && _ticks[i] <= tick)
                    _occupied[i] = false;
            }
        }

        /// <summary>Clears all retained values.</summary>
        public void Clear()
        {
            Array.Clear(_occupied, 0, _occupied.Length);
        }
    }

    /// <summary>Provides a bounded typed timeline for continuous network input.</summary>
    public sealed class NetworkInputTimeline<TInput>
        where TInput : struct, INetworkInput
    {
        private readonly PredictionHistory<NetworkInputFrame<TInput>> _history;

        /// <summary>Creates a timeline with fixed tick capacity.</summary>
        public NetworkInputTimeline(int capacity)
        {
            _history = new PredictionHistory<NetworkInputFrame<TInput>>(capacity);
        }

        /// <summary>Gets the maximum number of retained ticks.</summary>
        public int Capacity => _history.Capacity;

        /// <summary>Stores or replaces one input frame.</summary>
        public void Store(in NetworkInputFrame<TInput> frame)
        {
            _history.Store(frame.Tick, frame);
        }

        /// <summary>Gets one input frame when its exact tick remains retained.</summary>
        public bool TryGet(uint tick, out NetworkInputFrame<TInput> frame)
        {
            return _history.TryGet(tick, out frame);
        }

        /// <summary>Removes all input frames through the supplied tick.</summary>
        public void DiscardThrough(uint tick)
        {
            _history.DiscardThrough(tick);
        }

        /// <summary>Clears all retained input frames.</summary>
        public void Clear()
        {
            _history.Clear();
        }
    }
}
