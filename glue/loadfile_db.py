import sys

def main():
    try:
        # Your Glue job logic here
        print("Success")
        sys.exit(0)  # Exit with a success status code (0)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)  # Exit with a failure status code (non-zero)

if __name__ == "__main__":
    main()