use feldera_sqllib::*;
use crate::*;

pub fn text_array_drop_left(array: Option<Array<Option<SqlString>>>, num: Option<u32>) -> Result<Option<Array<Option<SqlString>>>, Box<dyn std::error::Error>> {
    match (array, num) {
        (Some(array1), Some(num1)) => {
            let mut array2 = Arc::unwrap_or_clone(array1);
            let num2 = (num1 as usize).min(array2.len());
            array2.drain(0..num2);
            Ok(Some(Arc::new(array2)))
        }
        _ => Ok(None),
    }
}
