using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace ZeroNsq.Helpers
{
    public static class StringHelpers
    {
        private const string NameValidationPattern = @"^[a-zA-Z0-9][.\w\-]*$";
        private const int MaxNsqNameLength = 64;
        private static readonly Regex NameValidationRegEx = new Regex(NameValidationPattern);

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

        public static string EnforceValidNsqName(this string input)
        {
            if (string.IsNullOrEmpty(input)) throw new ArgumentException("Input is null or empty.");

            input = input.Trim();

            if (input.Length > MaxNsqNameLength)
            {
                throw new ArgumentException("Name cannot exceed 64 characters.");
            }

            if (!NameValidationRegEx.IsMatch(input))
            {
                throw new ArgumentException("Names may only contain ASCII alpha-numeric characters, dashes and underscores.");
            }

            return input;
        }
    }
}
