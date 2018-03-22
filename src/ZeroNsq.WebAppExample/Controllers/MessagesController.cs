using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ZeroNsq.WebAppExample.Models;

namespace ZeroNsq.WebAppExample.Controllers
{
    public class MessagesController : Controller
    {
        private readonly string connectionString = "nsqd=http://127.0.0.1:4151;";
        [HttpGet]
        public IActionResult Index(bool? isAsync)
        {
            this.ViewBag.IsAsync = isAsync.GetValueOrDefault();
            IEnumerable<string> model = MessageSubscriber.ReceivedMessages.Take(10);
            return this.View(model);
        }

        [HttpPost]
        public IActionResult Publish(string message)
        {   
            using (var publisher = Publisher.CreateInstance(connectionString))
            {   
                publisher.Publish(MessageSubscriber.TopicName, message);
            }
            
            return this.RedirectToAction("Index", new { isAsync = false });
        }

        [HttpPost]
        public async Task<IActionResult> PublishAsync(string message)
        {
            using (var publisher = Publisher.CreateInstance(connectionString))
            {
                await publisher.PublishAsync(MessageSubscriber.TopicName, message);
            }

            return this.RedirectToAction("Index", new { isAsync = true });
        }
    }
}
