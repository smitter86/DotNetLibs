using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Qualer.Core.Lib.Extensions;

namespace Qualer.Core.Utils
{
    public static class ParallelExtensions
    {
        private static readonly SemaphoreSlim Semaphore = new SemaphoreSlim(4);

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <typeparam name="TCollectionData"></typeparam>
        /// <param name="processRecordsCountInSingleTask"></param>
        /// <param name="collection">Collection under parallel processing</param>
        /// <param name="processing">A job that would be performed to collection. Accepts records count in single task and starting id from each iteration</param>
        /// <param name="getUniqueId">Should return unique id</param>
        /// <returns></returns>
        public static async Task<List<TResult>> ProcessInParallel<TResult, TCollectionData>(
            this ICollection<TCollectionData> collection,
            int processRecordsCountInSingleTask,
            Func<int, TCollectionData, Task<List<TResult>>> processing,
            Func<TResult, int> getUniqueId)
        {
            var dic = new ConcurrentDictionary<int, TResult>();

            var collectionParts = collection
                .Split(processRecordsCountInSingleTask)
                .Select(item => item.ToArray());

            var tasks = collectionParts.Select(async item =>
            {
                await Semaphore.WaitAsync();
                var result = await processing(processRecordsCountInSingleTask, item.First());

                foreach (var data in result)
                {
                    var uniqueId = getUniqueId(data);
                    dic.TryAdd(uniqueId, data);
                }

                Semaphore.Release();
            });

            await Task.WhenAll(tasks);

            return dic.Select(item => item.Value).ToList();
        }
    }
}
