#nullable enable

namespace EmreErkanGames.AwaitableExtensions.Model.Abstract
{
    public interface IParallelAsyncLoopResult<T>
    {
        public bool IsCompleted { get; set; }
        public T BreakItem { get ; set; }
        public long? BreakIndex { get; set; }
        public bool IsBreakItem { get; set; }
        public bool HasBreak { get; }
    }
}
