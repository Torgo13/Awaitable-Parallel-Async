using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EmreErkanGames.AwaitableExtensions.Model.Abstract;
using EmreErkanGames.AwaitableExtensions.Model.Concrete;
using UnityEngine;

namespace EmreErkanGames.AwaitableExtensions.Core.Concrete
{
    public static class AwaitableParallel
    {
        private static ParallelOptions CreateDefaultParallelOptions()
        {
            return new ParallelOptions
            {
                CancellationToken = new CancellationTokenSource().Token,
                MaxDegreeOfParallelism = Environment.ProcessorCount * 2
            };
        }

        public static async Awaitable<IParallelAsyncLoopResult<T>> ForEachAsync<T>(IEnumerable<T> source, Func<T, IParallelAsyncLoopState, Awaitable> body, ParallelOptions parallelOptions)
        {
            var parallelAsyncLoopResult = new ParallelAsyncLoopResult<T>
            {
                IsCompleted = false,
                IsBreakItem = true
            };
            var disposed = 0L;
            var disposedLock = new object();
            var parallelCancellationTokenSource = new CancellationTokenSource();
            var completeSemphoreSlim = new SemaphoreSlim(1);
            var taskCountLimitsemaphoreSlim = new SemaphoreSlim(parallelOptions.MaxDegreeOfParallelism);
            var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(parallelOptions.CancellationToken,
                    parallelCancellationTokenSource.Token);
            var parallelAsyncLoopState = new ParallelAsyncLoopState(parallelCancellationTokenSource);
            await completeSemphoreSlim.WaitAsync(cancellationTokenSource.Token);
            var runningTaskCount = source.Count();
            if (runningTaskCount == 0)
            {
                completeSemphoreSlim.Release();
                taskCountLimitsemaphoreSlim.Release();
            }
            foreach (var item in source)
            {
                try
                {
                    await taskCountLimitsemaphoreSlim.WaitAsync(cancellationTokenSource.Token);
                }
                catch (Exception)
                {
                    // ignored
                }
                if (cancellationTokenSource.IsCancellationRequested)
                    break;
                var item2 = item;
                _ = AwaitableExtensions.RunOnThreadPool(async () =>
                {
                    if (Interlocked.Read(ref disposed) == 1)
                        return;
                    try
                    {
                        await body.Invoke(item2, parallelAsyncLoopState);
                        lock (disposedLock)
                        {
                            if (Interlocked.Read(ref disposed) == 0)
                            {
                                if (parallelCancellationTokenSource.IsCancellationRequested)
                                    parallelAsyncLoopResult.BreakItem = item2;
                            }
                        }
                    }
                    finally
                    {
                        lock (disposedLock)
                        {
                            if (Interlocked.Read(ref disposed) == 0)
                            {
                                taskCountLimitsemaphoreSlim.Release();
                                Interlocked.Decrement(ref runningTaskCount);
                                if (Interlocked.CompareExchange(ref runningTaskCount, -1, 0) == 0)
                                    completeSemphoreSlim.Release();
                            }
                        }
                    }
                }, false, cancellationToken: cancellationTokenSource.Token);
            }
            try
            {
                await completeSemphoreSlim.WaitAsync(cancellationTokenSource.Token);
            }
            catch (Exception)
            {
                // ignored
            }
            parallelAsyncLoopResult.IsCompleted = !parallelCancellationTokenSource.IsCancellationRequested;
            lock (disposedLock)
            {
                Interlocked.Increment(ref disposed);
                taskCountLimitsemaphoreSlim.Dispose();
                completeSemphoreSlim.Dispose();
                parallelCancellationTokenSource.Dispose();
                cancellationTokenSource.Dispose();
            }
            return parallelAsyncLoopResult;
        }

        public static async Awaitable<IParallelAsyncLoopResult<T>> ForEachAsync<T>(IEnumerable<T> source, Func<T, IParallelAsyncLoopState, Awaitable> body)
        {
            var parallelOptions = CreateDefaultParallelOptions();
            return await ForEachAsync(source, body, parallelOptions);
        }

        public static async Awaitable<IParallelAsyncLoopResult<T>> ForEachAsync<T>(IEnumerable<T> source, Func<T, Awaitable> body, ParallelOptions parallelOptions)
        {
            return await ForEachAsync(source, async (item, breakCancellationSource) => await body.Invoke(item), parallelOptions);
        }

        public static async Awaitable<IParallelAsyncLoopResult<T>> ForEachAsync<T>(IEnumerable<T> source, Func<T, Awaitable> body)
        {
            var parallelOptions = CreateDefaultParallelOptions();
            return await ForEachAsync(source, body, parallelOptions);
        }

        public static async Awaitable<IParallelAsyncLoopResult<long>> ForAsync(long fromInclusive, long toExclude, Func<long, IParallelAsyncLoopState, Awaitable> body, ParallelOptions parallelOptions)
        {
            var parallelAsyncLoopResult = new ParallelAsyncLoopResult<long>
            {
                IsCompleted = false,
                IsBreakItem = false
            };
            var disposed = 0L;
            var disposedLock = new object();
            var parallelCancellationTokenSource = new CancellationTokenSource();
            var completeSemphoreSlim = new SemaphoreSlim(1);
            var taskCountLimitsemaphoreSlim = new SemaphoreSlim(parallelOptions.MaxDegreeOfParallelism);
            var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(parallelOptions.CancellationToken,
                parallelCancellationTokenSource.Token);
            var parallelAsyncLoopState = new ParallelAsyncLoopState(parallelCancellationTokenSource);
            await completeSemphoreSlim.WaitAsync(cancellationTokenSource.Token);
            var runningTaskCount = toExclude - fromInclusive;
            if (runningTaskCount == 0)
            {
                completeSemphoreSlim.Release();
                taskCountLimitsemaphoreSlim.Release();
            }
            for (var index = fromInclusive; index < toExclude; index++)
            {
                try
                {
                    await taskCountLimitsemaphoreSlim.WaitAsync(cancellationTokenSource.Token);
                }
                catch (Exception)
                {
                    // ignored
                }
                if (cancellationTokenSource.IsCancellationRequested)
                    break;
                var item2 = index;
                _ = AwaitableExtensions.RunOnThreadPool(async () =>
                {
                    if (Interlocked.Read(ref disposed) == 1)
                        return;
                    try
                    {
                        await body.Invoke(item2, parallelAsyncLoopState);
                        lock (disposedLock)
                        {
                            if (Interlocked.Read(ref disposed) == 0)
                            {
                                if (parallelCancellationTokenSource.IsCancellationRequested)
                                    parallelAsyncLoopResult.BreakIndex = item2;
                            }
                        }
                    }
                    finally
                    {
                        lock (disposedLock)
                        {
                            if (Interlocked.Read(ref disposed) == 0)
                            {
                                taskCountLimitsemaphoreSlim.Release();
                                Interlocked.Decrement(ref runningTaskCount);
                                if (Interlocked.CompareExchange(ref runningTaskCount, -1, 0) == 0)
                                    completeSemphoreSlim.Release();
                            }
                        }
                    }
                }, false, cancellationToken: cancellationTokenSource.Token);
            }
            try
            {
                await completeSemphoreSlim.WaitAsync(cancellationTokenSource.Token);
            }
            catch (Exception)
            {
                // ignored
            }
            parallelAsyncLoopResult.IsCompleted = !parallelCancellationTokenSource.IsCancellationRequested;
            lock (disposedLock)
            {
                Interlocked.Increment(ref disposed);
                taskCountLimitsemaphoreSlim.Dispose();
                completeSemphoreSlim.Dispose();
                parallelCancellationTokenSource.Dispose();
                cancellationTokenSource.Dispose();
            }
            return parallelAsyncLoopResult;
        }

        public static async Awaitable<IParallelAsyncLoopResult<long>> ForAsync(long fromInclusive, long toExclude, Func<long, IParallelAsyncLoopState, Awaitable> body)
        {
            var parallelOptions = CreateDefaultParallelOptions();
            return await ForAsync(fromInclusive, toExclude, body, parallelOptions);
        }

        public static async Awaitable<IParallelAsyncLoopResult<long>> ForAsync(long fromInclusive, long toExclude, Func<long, Awaitable> body, ParallelOptions parallelOptions)
        {
            return await ForAsync(fromInclusive, toExclude, async (item, breakCancellationSource) => await body.Invoke(item), parallelOptions);
        }

        public static async Awaitable<IParallelAsyncLoopResult<long>> ForAsync(long fromInclusive, long toExclude, Func<long, Awaitable> body)
        {
            var parallelOptions = CreateDefaultParallelOptions();
            return await ForAsync(fromInclusive, toExclude, body, parallelOptions);
        }

        public static async Awaitable<IParallelAsyncLoopResult<int>> ForAsync(int fromInclusive, int toExclude,
            Func<int, IParallelAsyncLoopState, Awaitable> body, ParallelOptions parallelOptions)
        {
            var parallelAsyncLoopResult = new ParallelAsyncLoopResult<int>
            {
                IsCompleted = false,
                IsBreakItem = false
            };
            var parallelAsyncLoopResultLong = await ForAsync((long)fromInclusive, (long)toExclude, async (index, breakCancellationSource) => await body.Invoke((int)index, breakCancellationSource), parallelOptions);
            if (parallelAsyncLoopResultLong?.BreakItem != null)
                parallelAsyncLoopResult.BreakItem = (int)parallelAsyncLoopResultLong.BreakItem;
            if (parallelAsyncLoopResultLong != null)
                parallelAsyncLoopResult.IsCompleted = parallelAsyncLoopResultLong.IsCompleted;
            return parallelAsyncLoopResult;
        }

        public static async Awaitable<IParallelAsyncLoopResult<int>> ForAsync(int fromInclusive, int toExclude,
            Func<int, IParallelAsyncLoopState, Awaitable> body)
        {
            var parallelOptions = CreateDefaultParallelOptions();
            return await ForAsync(fromInclusive, toExclude, body, parallelOptions);
        }

        public static async Awaitable<IParallelAsyncLoopResult<int>> ForAsync(int fromInclusive, int toExclude,
            Func<int, Awaitable> body, ParallelOptions parallelOptions)
        {
            return await ForAsync(fromInclusive, toExclude, async (item, breakCancellationSource) => await body.Invoke(item), parallelOptions);
        }

        public static async Awaitable<IParallelAsyncLoopResult<int>> ForAsync(int fromInclusive, int toExclude,
            Func<int, Awaitable> body)
        {
            var parallelOptions = CreateDefaultParallelOptions();
            return await ForAsync(fromInclusive, toExclude, body, parallelOptions);
        }
    }

    public static class AwaitableExtensions
    {
        /// <see href="https://github.com/Cysharp/UniTask/blob/73a63b7f672b88f7e9992f6917eb458a8cbb6fa9/src/UniTask/Assets/Plugins/UniTask/Runtime/UniTask.Run.cs#L119"/>
        /// <summary>Run action on the threadPool and return to main thread if configureAwait = true.</summary>
        public static async Awaitable RunOnThreadPool(Func<Awaitable> action, bool configureAwait = true, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await Awaitable.BackgroundThreadAsync();

            cancellationToken.ThrowIfCancellationRequested();

            if (configureAwait)
            {
                try
                {
                    await action();
                }
                finally
                {
                    await Awaitable.NextFrameAsync();
                }
            }
            else
            {
                await action();
            }

            cancellationToken.ThrowIfCancellationRequested();
        }
    }
}