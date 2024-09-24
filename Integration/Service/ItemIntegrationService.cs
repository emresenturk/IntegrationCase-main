using Integration.Common;
using Integration.Backend;
using System.Data.Common;
namespace Integration.Service;

public sealed class ItemIntegrationService
{

    private readonly Redlock.CSharp.Redlock distributedLock;
    private Redlock.CSharp.Lock lockObject;

    public ItemIntegrationService(Redlock.CSharp.Redlock distributedLock)
    {
        this.distributedLock = distributedLock;
    }

    //This is a dependency that is normally fulfilled externally.
    private ItemOperationBackend ItemIntegrationBackend { get; set; } = new();

    // This is called externally and can be called multithreaded, in parallel.
    // More than one item with the same content should not be saved. However,
    // calling this with different contents at the same time is OK, and should
    // be allowed for performance reasons.
    public Result SaveItem(string itemContent)
    {
        if(distributedLock.Lock(itemContent, TimeSpan.FromMinutes(1), out lockObject))
        {
            // Check the backend to see if the content is already saved.
            if (ItemIntegrationBackend.FindItemsWithContent(itemContent).Count != 0)
            {
                return new Result(false, $"Duplicate item received with content {itemContent}.");
            }

            Console.WriteLine($"saving {itemContent}");

            var item = ItemIntegrationBackend.SaveItem(itemContent);
            
            if(lockObject != null)
            {
                distributedLock.Unlock(lockObject);
            }

            return new Result(true, $"Item with content {itemContent} saved with id {item.Id}");
        }

        return new Result(false, $"Duplicate item received with content {itemContent}.");
    }

    public List<Item> GetAllItems()
    {
        return ItemIntegrationBackend.GetAllItems();
    }
}