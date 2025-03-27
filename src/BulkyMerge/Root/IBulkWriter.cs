using System.Threading.Tasks;
namespace BulkyMerge.Root;

internal interface IBulkWriter
{
    void Write<T>(string destination, MergeContext<T> context);
    Task WriteAsync<T>(string destination, MergeContext<T> context);
}