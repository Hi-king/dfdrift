import pandas as pd
import dfvalidate
from datetime import datetime

def main():
    validator = dfvalidate.DfValidator()
    
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    df = pd.DataFrame({
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        current_time: [1, 2, 3]
    })
    
    validator.validate(df)
    print("First validation complete")

if __name__ == "__main__":
    main()