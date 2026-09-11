#if NETSTANDARD2_0
// The nullable-analysis attributes ship in the BCL from netstandard2.1 / .NET Core 3.0 on.
// On netstandard2.0 they only need to exist; the analysis itself runs in the net8.0 build.
namespace System.Diagnostics.CodeAnalysis
{
    [AttributeUsage(AttributeTargets.Parameter, Inherited = false)]
    internal sealed class NotNullWhenAttribute : Attribute
    {
        public NotNullWhenAttribute(bool returnValue) { ReturnValue = returnValue; }

        public bool ReturnValue { get; }
    }

    [AttributeUsage(AttributeTargets.Parameter, Inherited = false)]
    internal sealed class MaybeNullWhenAttribute : Attribute
    {
        public MaybeNullWhenAttribute(bool returnValue) { ReturnValue = returnValue; }

        public bool ReturnValue { get; }
    }
}
#endif
