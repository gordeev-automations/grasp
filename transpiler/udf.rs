use feldera_sqllib::*;
use crate::*;

pub fn grasp_text_array_drop_left(array: Option<Array<Option<SqlString>>>, num: Option<u32>) -> Result<Option<Array<Option<SqlString>>>, Box<dyn std::error::Error>> {
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

pub fn grasp_text_array_drop_right(array: Option<Array<Option<SqlString>>>, num: Option<u32>) -> Result<Option<Array<Option<SqlString>>>, Box<dyn std::error::Error>> {
    match (array, num) {
        (Some(array1), Some(num1)) => {
            let mut array2 = Arc::unwrap_or_clone(array1);
            let num2 = (array2.len() - (num1 as usize).min(array2.len()));
            array2.drain(num2..array2.len());
            Ok(Some(Arc::new(array2)))
        }
        _ => Ok(None),
    }
}

pub fn grasp_variant_array_drop_sides(array: Option<Array<Option<Variant>>>, drop_on_left: Option<u32>, drop_on_right: Option<u32>) -> Result<Option<Array<Option<Variant>>>, Box<dyn std::error::Error>> {
    match (array, drop_on_left, drop_on_right) {
        (Some(array1), Some(on_left), Some(on_right)) => {
            let mut array2 = Arc::unwrap_or_clone(array1);
            let right2 = (array2.len() - (on_right as usize).min(array2.len()));
            let left2 = (on_left as usize).min(array2.len());
            array2.drain(right2..array2.len());
            array2.drain(0..left2);
            Ok(Some(Arc::new(array2)))
        }
        _ => Ok(None),
    }
}