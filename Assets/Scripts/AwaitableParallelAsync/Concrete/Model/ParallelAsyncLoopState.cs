using System.Threading;
using EmreErkanGames.AwaitableExtensions.Model.Abstract;

namespace EmreErkanGames.AwaitableExtensions.Model.Concrete
{
    public class ParallelAsyncLoopState : IParallelAsyncLoopState
    {
        private readonly CancellationTokenSource _cancellationTokenSource;

        public ParallelAsyncLoopState(CancellationTokenSource cancellationTokenSource)
        {
            _cancellationTokenSource = cancellationTokenSource;
        }

        public void Break()
        {
            if(_cancellationTokenSource.Token.CanBeCanceled)
                _cancellationTokenSource.Cancel();
        }
    }
}
