using NRedi2Read.Helpers;
using NRedi2Read.Models;
using NRediSearch;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace NRedi2Read.Services
{
    public class CartService
    {
        private const string CART_INDEX_NAME = "cart-idx";

        private readonly IDatabase _db;
        private readonly Client _searchClient;
        private readonly UserService _userService;

        public CartService(IConnectionMultiplexer multiplexer, UserService userService)
        {     
            _db = multiplexer.GetDatabase();
            _searchClient = new Client(CART_INDEX_NAME, _db);
            _userService = userService;
        }

        /// <summary>
        /// Get's a cart
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public async Task<Cart> Get(string id)
        {            
            var result = await _db.HashGetAllAsync(CartKey(id));
            var cart = RedisHelper.ConvertFromRedis<Cart>(result);
            cart.Items = ParseCartItems(result).ToArray();
            return cart;
        }

        private IEnumerable<CartItem> ParseCartItems(IEnumerable<HashEntry> entries)
        {
            var uniqueIds = new HashSet<string>();
            entries
                .Where(s => s.Name.ToString().Split(':').Length > 2)
                .Select(s => s.Name.ToString().Split(":")[1]).ToList()
                .ForEach(x => uniqueIds.Add(x));
            foreach(var id in uniqueIds)
            {
                var isbn = entries.FirstOrDefault(e => e.Name == $"items:{id}:Isbn").Value;
                var price = entries.FirstOrDefault(e => e.Name == $"items:{id}:Price").Value;
                var quantity = (long)entries.FirstOrDefault(e => e.Name == $"items:{id}:Quantity").Value;
                yield return new CartItem { Isbn = isbn, Quantity = quantity, Price=price};
            }
        }

        /// <summary>
        /// adds an item to a cart
        /// </summary>
        /// <param name="id"></param>
        /// <param name="item"></param>
        /// <exception cref="InvalidOperationException">Thrown if cart is already closed</exception>
        /// <returns></returns>
        public async Task AddToCart(string id, CartItem item)
        {
            var closed = bool.Parse(await _db.HashGetAsync(CartKey(id), "Closed"));
            if (closed)
            {
                throw new InvalidOperationException("Cart has already been closed out");
            }
            var key = CartKey(id);
            await _db.HashSetAsync(key, item.AsHashEntries(CartItemKey(id, item.Isbn)).ToArray());            
        }

        /// <summary>
        /// Removes an Item from a cart
        /// </summary>
        /// <param name="id"></param>
        /// <param name="isbn"></param>
        /// <returns></returns>
        public async Task RemoveFromCart(string id, string isbn)
        {
            await _db.HashDeleteAsync(CartKey(id), CartItemHashFields(id,isbn));
        }

        /// <summary>
        /// Creates a new Cart
        /// </summary>
        /// <param name="userId"></param>
        /// <exception cref="RedisKeyNotFoundException">Thrown if user id isn't found</exception>
        /// <returns></returns>
        public async Task<string> Create(string userId)
        {
            var currentCart = await GetCartForUser(userId);
            if (currentCart != null)
            {
                return currentCart.Id;
            }
            var user = await _userService.Read(userId);
            var newCartId = (await _db.StringIncrementAsync("Cart:id")).ToString(); // get the cart id by incrmenting the current highestcart id
            var cart = new Cart
            {
                Id = newCartId,
                UserId = userId, 
                Items = null
            };
            await _db.HashSetAsync(CartKey(cart.Id), cart.AsHashEntries().ToArray());            
            return cart.Id;
        }

        /// <summary>
        /// Checks out a user with a given cart, adds all their items to the user
        /// and closes out the cart
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public async Task<bool> Checkout(string id)
        {
            var cart = await Get(id);
            
            await _userService.AddBooks(cart.UserId, new List<string>(cart.Items.Select(x => x.Isbn)).ToArray());            ;
            await _db.HashSetAsync(CartKey(id), "Closed", true);
            return true;
        }

        /// <summary>
        /// Returns cart that has not been closed for user, if one exists, otherwise returns null
        /// </summary>
        /// <param name="userId"></param>
        /// <returns></returns>
        public async Task<Cart> GetCartForUser(string userId)
        {            
            var query = new Query($"" +
                $"@UserId:{{{RedisHelper.RediSearchEscape(userId)}}} " +
                $"@Closed:{{{false}}}");
            query.ReturnFields("Id");            
            var result = await _searchClient.SearchAsync(query);
            if (result.Documents.Count < 1)
            {
                return null;
            }            
            var idStr = result.Documents[0]["Id"].ToString();
            return await Get(idStr);
        }

        public void CreateCartIndex()
        {
            try
            {
                _db.Execute("FT.DROPINDEX", "cart-idx");
            }
            catch (Exception)
            {
                //do nothing, the index didn't exist
            }
            var schema = new Schema();
            schema.AddTagField("UserId");
            schema.AddTagField("Closed");
            var options = new Client.ConfiguredIndexOptions(new Client.IndexDefinition(prefixes: new[] { "Cart:" }));
            _searchClient.CreateIndex(schema, options);
        }

        private RedisValue[] CartItemHashFields(string cartId, string isbn)
        {
            return new RedisValue[] { $"{CartItemKey(cartId, isbn)}:Isbn", $"{CartItemKey(cartId, isbn)}:Price", $"{CartItemKey(cartId, isbn)}:Quantity" };
        }        

        private RedisKey CartKey(string id)
        {
            return new(new Cart().GetType().Name + ":" + id);
        }

        private RedisKey CartItemKey(string cartId, string isbn)
        {
            return $"items:{isbn}:";
        }
    }
}