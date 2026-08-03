namespace UniGame.StaticEcs.Network
{
    /// <summary>Defines the local ownership role of a replication scope.</summary>
    public enum ScopeRole : byte
    {
        /// <summary>Captures state from locally owned chunks.</summary>
        Authority = 0,
        /// <summary>Applies state into remotely owned chunks.</summary>
        Replica = 1
    }

    /// <summary>Describes the result of capturing one canonical full snapshot.</summary>
    public enum CaptureResult : byte
    {
        /// <summary>The snapshot was captured and its lease belongs to the caller.</summary>
        Success = 0,
        /// <summary>The scope is not an authority scope.</summary>
        WrongRole = 1,
        /// <summary>The scope is disposed, malformed, or its world topology drifted.</summary>
        ScopeInvalid = 2,
        /// <summary>A mapped entity conflicts with the scope or canonical capture set.</summary>
        EntityConflict = 3,
        /// <summary>An entity identity or entity kind is invalid.</summary>
        InvalidEntity = 4,
        /// <summary>Version one cannot encode a disabled tag, relation, or multi value.</summary>
        DisabledUnsupported = 5,
        /// <summary>A relation target is absent from the same full snapshot.</summary>
        MissingTarget = 6,
        /// <summary>A protocol, schema, or collection bound was exceeded.</summary>
        LimitExceeded = 7,
        /// <summary>A registered codec could not encode its value exactly.</summary>
        CodecFailed = 8
    }

    /// <summary>Describes the result of applying one staged full snapshot.</summary>
    public enum ApplyResult : byte
    {
        /// <summary>The complete snapshot was applied.</summary>
        Success = 0,
        /// <summary>The scope is not a replica scope.</summary>
        WrongRole = 1,
        /// <summary>The staged payload is not a full snapshot.</summary>
        WrongPayload = 2,
        /// <summary>The staged payload was validated by a different schema.</summary>
        SchemaMismatch = 3,
        /// <summary>The scope is disposed, malformed, or its world topology drifted.</summary>
        ScopeInvalid = 4,
        /// <summary>An existing physical occupant is foreign or cannot be replaced.</summary>
        EntityConflict = 5,
        /// <summary>An entity identity, entity kind, record state, or segment kind is invalid.</summary>
        InvalidEntity = 6,
        /// <summary>A relation target is absent from the same full snapshot.</summary>
        MissingTarget = 7,
        /// <summary>A protocol, schema, or collection bound was exceeded.</summary>
        LimitExceeded = 8
    }
}
