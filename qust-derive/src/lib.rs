#![allow(unused_imports)]
use std::env::args;

use proc_macro::{self, TokenStream as ts1};
use quote::{quote, quote_spanned, format_ident};
use syn::{Attribute, ItemFn, Meta, MetaNameValue, ReturnType, TraitItem};
use syn::{
    parse_macro_input, DeriveInput, Generics, ImplItem, Data, Fields, 
    spanned::Spanned, Index, Ident, GenericParam, parse_quote, ItemImpl, token::Trait, ItemTrait
};
use proc_macro2::TokenStream as ts2;
use synstructure::{AddBounds, Structure};

fn derive_as_ref(mut structure: Structure<'_>) -> ts2 {
    structure
        .underscore_const(true)
        .add_bounds(AddBounds::None)
        .gen_impl(quote! {
            gen impl ::core::convert::AsRef<Self> for @Self {
                fn as_ref(&self) -> &Self {
                    &self
                }
            }
        })
}
synstructure::decl_derive!([AsRef] => derive_as_ref);


#[proc_macro_attribute]
pub fn ta_derive(_metadata: ts1, input: ts1) -> ts1 {
    let input_token: ts2 = input.into();
    let output = quote! {
        #[derive(Debug, Clone, Serialize, Deserialize, AsRef)]
        #input_token
    };
    output.into()
}

#[proc_macro_attribute]
pub fn ta_derive2(_metadata: ts1, input: ts1) -> ts1 {
    let input_token: ts2 = input.into();
    let output = quote! {
        #[derive(PathDebug, Clone, Serialize, Deserialize, AsRef)]
        #input_token
    };
    output.into()
}

#[proc_macro_attribute]
pub fn clone_trait(_metadata: ts1, input: ts1) -> ts1 {
    let input_token: ItemTrait = syn::parse(input).unwrap();
    let name = input_token.ident.clone();
    let name_str = name.to_string();
    let name_to_box = format_ident!("{}_box", name_str.to_lowercase());
    let type_ident = format_ident!("{}Box", name_str);
    let visi = input_token.vis;
    let item = input_token.items.into_iter();
    let attrs = input_token.attrs.into_iter();
    let gener = input_token.generics;
    let output = quote! {
        #[typetag::serde(tag = #name_str)]
        #(#attrs)*
        #visi trait #name #gener: DynClone + Send + Sync + std::fmt::Debug + 'static {
            #(#item)*
            fn #name_to_box(&self) -> Box<dyn #name>
            where
            Self: Clone,
            {
                Box::new(self.clone())
            }
        }
        clone_trait_object!(#gener #name #gener);
        impl PartialEq for Box<dyn #name> {
            fn eq(&self, other: &Self) -> bool {
                format!("{:?}", self) == format!("{:?}", other)
            }
        }
        pub type #type_ident = Box<dyn #name>;
    };
    proc_macro::TokenStream::from(output)
}

// #[proc_macro_attribute]
// pub fn typetag_name(_metadata: ts1, input: ts1) -> ts1 {
//     let module_name = module_path!();
//     let mut input_token: ItemImpl = syn::parse(input).unwrap();
//     input_token.items.push(parse_quote! {
//         #[doc(hidden)]
//         fn typetag_name(&self) -> &'static str {
//             #name
//         }
//     });
//     let mut name_str = String::default();
//     for t in input_token.items.iter() {
//         if let ImplItem::Type(tz) = t {
//             name_str = tz.ident.to_string();
//         }
//     }
//     let tag_name = format!("{}::{:?}", module_name, name_str);
//     quote! {
//         #[typetag::serde(name = #tag_name)]
//         #input_token
//     }.into()
// }

// fn augment_impl(input: &mut ItemImpl, name: &ts2) {
//     input.items.push(parse_quote! {
//         #[doc(hidden)]
//         fn typetag_name(&self) -> &'static str {
//             #name
//         }
//     });
//     input.items.push(parse_quote! {
//         #[doc(hidden)]
//         fn typetag_deserialize(&self) {}
//     });
// }

#[proc_macro_derive(PathDebug)]
pub fn path_debug(input: ts1) -> ts1 {
    let ast: DeriveInput = syn::parse(input).unwrap();
    let name = ast.ident.clone();
    let b = get_fields_name(&ast.ident, &ast.data);
    let generics = add_trait_bounds(ast.generics);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let output = quote! {
        impl #impl_generics std::fmt::Debug for #name #ty_generics #where_clause {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f
                #b
                .finish()
            }
        }

    };
    proc_macro::TokenStream::from(output)
}


fn add_trait_bounds(mut generics: Generics) -> Generics {
    for param in &mut generics.params {
        if let GenericParam::Type(ref mut type_param) = *param {
            type_param.bounds.push(parse_quote!(std::fmt::Debug));
        }
    }
    generics
}

fn get_fields_name(type_name: &Ident, data: &Data) -> ts2 {
    let type_string = type_name.to_string();
    match *data {
        Data::Struct(ref data) => {
            match data.fields {
                Fields::Named(ref fields) => {
                    let recurse = fields.named.iter().map(|f| {
                        let name = &f.ident;
                        let name_string = name.as_ref().map_or_else(String::default, |v| v.to_string());
                        quote! { field(#name_string, &self.#name) }
                    });
                    quote! {
                        .debug_struct(&format!("{}::{}", module_path!().split("::").last().unwrap(), #type_string))
                        #(.#recurse)*
                    }
                }
                Fields::Unnamed(ref fields) => {
                    let recurse = fields.unnamed.iter().enumerate().map(|(i, _f)| {
                        let index = Index::from(i);
                        quote! { field(&self.#index) }
                    });
                    quote! {
                        .debug_tuple(&format!("{}::{}", module_path!().split("::").last().unwrap(), #type_string))
                        #(.#recurse)*
                    }
                }
                Fields::Unit => {
                    quote! {
                        .debug_struct(&format!("{}::{}", module_path!().split("::").last().unwrap(), #type_string))
                    }
                }
            }
        }
        Data::Enum(_) | Data::Union(_) => unimplemented!(),
    }
}

#[proc_macro_attribute]
pub fn lazy_init(attr: ts1, item: ts1) -> ts1 {
    let input = parse_macro_input!(item as ItemTrait);
    let di_expr = parse_macro_input!(attr as syn::Expr);

    let trait_name = &input.ident;
    let mut methods = quote!{};

    for item in input.items.clone() {
        if let TraitItem::Fn(trait_item_fn) = item {
            let fn_name = trait_item_fn.sig.ident;
            let fn_name_lazy = format_ident!("{}_lazy", fn_name);
            let ReturnType::Type(_, return_type) = trait_item_fn.sig.output else { panic!("return type") };
            methods.extend(quote! {
                fn #fn_name_lazy(&self) -> #return_type {
                    let mut cond_fn: Option<#return_type> = None;
                    Box::new(move |data| {
                        cond_fn.get_or_insert_with(|| self.#fn_name(#di_expr))(data)
                    })
                }
            });
            break;
        }
    }

    let items = &input.items;
    let attrs = &input.attrs;

    let expanded = quote! {
        #(#attrs)*
        pub trait #trait_name {
            #(#items)*
            #methods
        }
    };
    ts1::from(expanded)
}