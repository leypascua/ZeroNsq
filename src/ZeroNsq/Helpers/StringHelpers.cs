using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;

namespace ZeroNsq
{
    public static class StringHelpers
    {
        /// <summary>
        /// Calculates the MD5 hash of a given string
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static string Md5(this string input)
        {
            input = input.ToLowerInvariant();

            // Use input string to calculate MD5 hash
            using (MD5 md5 = System.Security.Cryptography.MD5.Create())
            {
                byte[] inputBytes = System.Text.Encoding.ASCII.GetBytes(input);
                byte[] hashBytes = md5.ComputeHash(inputBytes);

                // Convert the byte array to hexadecimal string
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < hashBytes.Length; i++)
                {
                    sb.Append(hashBytes[i].ToString("X2"));
                }

                return sb.ToString().ToLowerInvariant();
            }   
        }
    }
}
