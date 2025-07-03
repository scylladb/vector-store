use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DataEnum, DeriveInput, parse_macro_input};

#[proc_macro_derive(ToEnumSchema)]
pub fn derive_to_enum_schema(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let enum_name = &input.ident;
    let variants = match &input.data {
        Data::Enum(DataEnum { variants, .. }) => variants,
        _ => panic!("ToEnumSchema can only be derived for enums"),
    };

    let enum_doc = get_enum_doc(&input).unwrap_or(enum_name.to_string());

    let (variant_values, variant_docs): (Vec<_>, Vec<_>) = variants
        .iter()
        .map(|variant| {
            let variant_ident = &variant.ident;
            let doc = get_variant_doc(variant).unwrap_or_default();
            (quote! { #enum_name::#variant_ident }, doc)
        })
        .unzip();

    let schema = quote! {
        let enum_values = vec![#(#variant_values),*];
        let enum_descriptions = vec![#(#variant_docs.to_string()),*];
        let enum_names: Vec<String> = enum_values.iter().map(|variant| {
            serde_json::to_value(variant).unwrap().as_str().unwrap().to_string()
        }).collect();

        let extension = utoipa::openapi::extensions::Extensions::builder()
            .add(
                "x-enum-descriptions",
                enum_descriptions,
            )
            .build();

        utoipa::openapi::schema::Object::builder()
            .schema_type(utoipa::openapi::schema::SchemaType::new(
                utoipa::openapi::schema::Type::String,
            ))
            .description(Some(#enum_doc))
            .enum_values(Some(enum_names))
            .extensions(Some(extension))
            .build()
            .into()
    };

    let expanded = quote! {
        impl utoipa::PartialSchema for #enum_name {
            fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
                #schema
            }
        }
        impl utoipa::ToSchema for #enum_name {}
    };

    TokenStream::from(expanded)
}

fn get_variant_doc(variant: &syn::Variant) -> Option<String> {
    variant.attrs.iter().find_map(|attr| {
        if attr.path().is_ident("doc") {
            if let syn::Meta::NameValue(nv) = &attr.meta {
                if let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(lit),
                    ..
                }) = &nv.value
                {
                    return Some(lit.value().trim().to_string());
                }
            }
        }
        None
    })
}

fn get_enum_doc(input: &DeriveInput) -> Option<String> {
    let doc = input
        .attrs
        .iter()
        .filter(|attr| matches!(attr.meta, syn::Meta::NameValue(ref nv) if nv.path.is_ident("doc")))
        .filter_map(|attr| {
            if let syn::Meta::NameValue(nv) = &attr.meta {
                if let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(lit),
                    ..
                }) = &nv.value
                {
                    let value = lit.value().trim().to_string();
                    if !value.is_empty() {
                        return Some(value);
                    }
                }
            }
            None
        })
        .collect::<Vec<_>>()
        .join("\n");

    if doc.is_empty() {
        None
    } else {
        Some(doc.trim().to_string())
    }
}
