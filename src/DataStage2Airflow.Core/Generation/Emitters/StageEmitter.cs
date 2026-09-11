using System;
using System.Collections.Generic;
using System.Linq;
using DataStage2Airflow.Model;

namespace DataStage2Airflow.Generation.Emitters
{
    /// <summary>Writes the Python for one kind of stage.</summary>
    internal abstract class StageEmitter
    {
        /// <summary>How the stage is implemented, for the report.</summary>
        public abstract string Implementation { get; }

        public abstract bool Handles(Stage stage, JobKind kind);

        public abstract void Emit(StageContext c);

        protected static bool TypeIs(Stage stage, params string[] names) =>
            names.Any(n => string.Equals(stage.StageType, n, StringComparison.OrdinalIgnoreCase)
                        || string.Equals(stage.OleType, n, StringComparison.OrdinalIgnoreCase));

        protected static bool TypeContains(Stage stage, params string[] fragments) =>
            fragments.Any(f => stage.StageType.IndexOf(f, StringComparison.OrdinalIgnoreCase) >= 0);

        /// <summary>Key column names from repeated "key" properties.</summary>
        protected static List<string> Keys(PropertySet properties, string name = "key") =>
            properties.GetAll(name).Select(n => n.Value.Trim()).Where(v => v.Length > 0).ToList();
    }

    internal static class StageEmitterRegistry
    {
        private static readonly StageEmitter[] Emitters =
        {
            new SequentialFileEmitter(),
            new DataSetEmitter(),
            new HashedFileEmitter(),
            new TransformerEmitter(),
            new LookupEmitter(),
            new JoinEmitter(),
            new MergeEmitter(),
            new AggregatorEmitter(),
            new ServerAggregatorEmitter(),
            new SortEmitter(),
            new ServerSortEmitter(),
            new RemoveDuplicatesEmitter(),
            new FunnelEmitter(),
            new CopyEmitter(),
            new FilterEmitter(),
            new SwitchEmitter(),
            new HeadTailEmitter(),
            new SampleEmitter(),
            new PeekEmitter(),
            new RowGeneratorEmitter(),
            new SurrogateKeyEmitter(),
            new ChangeCaptureEmitter(),
            new ModifyEmitter(),
            new PassThroughEmitter(),
            new DatabaseEmitter(),
        };

        public static StageEmitter? Find(Stage stage, JobKind kind) => Emitters.FirstOrDefault(e => e.Handles(stage, kind));
    }
}
