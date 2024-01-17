use proc_macro::{Span, TokenStream};
use quote::quote;
use syn::{bracketed, parse::Parse, Error, ExprClosure, Ident, Index, LitBool, Token};

pub fn action_impl(token_stream: TokenStream) -> TokenStream {
    let MacroParse {
        exchange_name,
        action_type,
        call_type,
        log_types,
        exchange_mod_name,
        give_logs,
        give_returns,
        call_function,
        give_calldata,
    } = syn::parse2(token_stream.into()).unwrap();

    let mut option_parsing = Vec::new();

    let (log_idx, log_types): (Vec<Index>, Vec<Ident>) = log_types
        .into_iter()
        .enumerate()
        .map(|(i, n)| (Index::from(i), n))
        .unzip();

    let a = call_type.to_string();
    let decalled = Ident::new(&a[..a.len() - 4], Span::call_site().into());

    if give_calldata {
        option_parsing.push(quote!(
                let call_data = crate::enum_unwrap!(data, #exchange_mod_name, #decalled);
        ));
    }

    if give_logs {
        option_parsing.push(quote!(
            let log_data =
            (
                #(
                    {
                    let log = &logs[#log_idx];
                    <crate::#exchange_mod_name::#log_types as ::alloy_sol_types::SolEvent>
                        ::decode_log_data(&log.data, false).ok()?
                    }

                ),*
            );
        ));
    }

    if give_returns {
        option_parsing.push(quote!(
                let return_data = <crate::#exchange_mod_name::#call_type
                as alloy_sol_types::SolCall>
                ::abi_decode_returns(&return_data, false).map_err(|e| {
                    tracing::error!("return data failed to decode {:#?}", return_data);
                    e
                }).unwrap();
        ));
    }

    let fn_call = match (give_calldata, give_logs, give_returns) {
        (true, true, true) => {
            quote!(
            (#call_function)(
                index,
                from_address,
                target_address,
                call_data,
                return_data,
                log_data, db_tx
                )
            )
        }
        (true, true, false) => {
            quote!(
                (#call_function)(index, from_address, target_address, call_data, log_data, db_tx)
            )
        }
        (true, false, true) => {
            quote!(
                (#call_function)(index, from_address, target_address, call_data, return_data, db_tx)
            )
        }
        (true, false, false) => {
            quote!(
                (#call_function)(index, from_address, target_address, call_data, db_tx)
            )
        }
        (false, true, true) => {
            quote!(
                (#call_function)(index, from_address, target_address, return_data, log_data, db_tx)
            )
        }
        (false, false, true) => {
            quote!(
                (#call_function)(index, from_address, target_address, return_data, db_tx)
            )
        }
        (false, true, false) => {
            quote!(
                (#call_function)(index, from_address, target_address, log_data, db_tx)
            )
        }
        (false, false, false) => {
            quote!(
                (#call_function)(index, from_address, target_address, db_tx)
            )
        }
    };

    quote! {

        #[derive(Debug, Default)]
        pub struct #exchange_name;

        impl crate::IntoAction for #exchange_name {
            fn get_signature(&self) -> [u8; 4] {
                <#call_type as alloy_sol_types::SolCall>::SELECTOR
            }

            #[allow(unused)]
            fn decode_trace_data(
                &self,
                index: u64,
                data: crate::StaticReturnBindings,
                return_data: ::alloy_primitives::Bytes,
                from_address: ::alloy_primitives::Address,
                target_address: ::alloy_primitives::Address,
                logs: &Vec<::alloy_primitives::Log>,
                db_tx: &::brontes_database_libmdbx::implementation::tx::LibmdbxTx<
                ::reth_db::mdbx::RO
                >,
            ) -> Option<::brontes_types::normalized_actions::Actions> {
                #(#option_parsing)*
                Some(::brontes_types::normalized_actions::Actions::#action_type(#fn_call?))
            }
        }
    }
    .into()
}

struct MacroParse {
    // required for all
    exchange_name: Ident,
    action_type:   Ident,
    log_types:     Vec<Ident>,
    call_type:     Ident,

    /// for call data decoding
    exchange_mod_name: Ident,
    /// wether we want logs or not
    give_logs:         bool,
    /// wether we want return data or not
    give_returns:      bool,
    give_calldata:     bool,

    /// The closure that we use to construct the normalized type
    call_function: ExprClosure,
}

impl Parse for MacroParse {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let exchange_name: Ident = input.parse()?;
        input.parse::<Token![,]>()?;
        let action_type: Ident = input.parse()?;
        input.parse::<Token![,]>()?;
        let call_type: Ident = input.parse()?;
        input.parse::<Token![,]>()?;

        let mut log_types = Vec::new();

        let content;
        bracketed!(content in input);

        loop {
            let Ok(log_type) = content.parse::<Ident>() else {
                break;
            };
            log_types.push(log_type);

            let Ok(_) = content.parse::<Token![,]>() else {
                break;
            };
        }

        input.parse::<Token![,]>()?;
        let exchange_mod_name: Ident = input.parse()?;

        let mut logs = false;
        let mut return_data = false;
        let mut call_data = false;

        input.parse::<Token![,]>()?;

        while !input.peek(Token![|]) {
            let arg: Ident = input.parse()?;
            input.parse::<Token![:]>()?;
            let enabled: LitBool = input.parse()?;

            match arg.to_string().to_lowercase().as_str() {
                "logs" => logs = enabled.value(),
                "call_data" => call_data = enabled.value(),
                "return_data" => return_data = enabled.value(),
                _ => {
                    return Err(Error::new(
                        arg.span(),
                        format!(
                            "{} is not a valid config option, valid options are: \n logs , \
                             call_data, return_data",
                            arg,
                        ),
                    ))
                }
            }
            input.parse::<Token![,]>()?;
        }
        // no data enabled
        let call_function: ExprClosure = input.parse()?;

        if call_function.asyncness.is_some() {
            return Err(syn::Error::new(input.span(), "closure cannot be async"))
        }

        if !input.is_empty() {
            return Err(syn::Error::new(
                input.span(),
                "There should be no values after the call function",
            ))
        }

        if call_function.asyncness.is_some() {
            return Err(syn::Error::new(input.span(), "closure cannot be async"))
        }

        if !input.is_empty() {
            return Err(syn::Error::new(
                input.span(),
                "There should be no values after the call function",
            ))
        }

        Ok(Self {
            give_returns: return_data,
            log_types,
            call_function,
            give_logs: logs,
            give_calldata: call_data,
            call_type,
            action_type,
            exchange_name,
            exchange_mod_name,
        })
    }
}

pub fn action_dispatch(input: TokenStream) -> TokenStream {
    let ActionDispatch { struct_name, rest } = syn::parse2(input.into()).unwrap();

    if rest.is_empty() {
        panic!("need more than one entry");
    }

    let (mut i, name): (Vec<Index>, Vec<Ident>) = rest
        .into_iter()
        .enumerate()
        .map(|(i, n)| (Index::from(i), n))
        .unzip();
    i.remove(0);

    quote!(
        #[derive(Default, Debug)]
        pub struct #struct_name(#(pub #name,)*);


        impl crate::ActionCollection for #struct_name {

            fn dispatch(
                &self,
                sig: &[u8],
                index: u64,
                data: crate::StaticReturnBindings,
                return_data: ::alloy_primitives::Bytes,
                from_address: ::alloy_primitives::Address,
                target_address: ::alloy_primitives::Address,
                logs: &Vec<::alloy_primitives::Log>,
                db_tx: &::brontes_database_libmdbx::implementation::tx::LibmdbxTx<
                    ::reth_db::mdbx::RO
                >,
                block: u64,
                tx_idx: u64,
            ) -> Option<(
                    ::brontes_pricing::types::PoolUpdate,
                    ::brontes_types::normalized_actions::Actions
                )> {
                let hex_selector = ::alloy_primitives::Bytes::copy_from_slice(sig);

                if sig == crate::IntoAction::get_signature(&self.0) {
                    return crate::IntoAction::decode_trace_data(
                            &self.0,
                            index,
                            data,
                            return_data,
                            from_address,
                            target_address,
                            logs,
                            db_tx
                        ).map(|res| {
                        (::brontes_pricing::types::PoolUpdate {
                            block,
                            tx_idx,
                            logs: logs.clone(),
                            action: res.clone()
                        },
                        res)}).or_else(|| {
                            ::tracing::error!(
                                "classifier failed on function sig: {:?} for address: {:?}",
                                ::malachite::strings::ToLowerHexString::to_lower_hex_string(
                                    &hex_selector
                                ),
                                target_address.0,
                            );
                            None
                        })

                }
                #( else if sig == crate::IntoAction::get_signature(&self.#i) {
                     return crate::IntoAction::decode_trace_data(
                            &self.#i,
                            index,
                            data,
                            return_data,
                            from_address,
                            target_address,
                            logs,
                            db_tx
                    ).map(|res| {
                        (::brontes_pricing::types::PoolUpdate {
                            block,
                            tx_idx,
                            logs: logs.clone(),
                            action: res.clone()
                        },
                        res)}).or_else(|| {
                            ::tracing::error!(
                                "classifier failed on function sig: {:?} for address: {:?}",
                                ::malachite::strings::ToLowerHexString::to_lower_hex_string(
                                    &hex_selector
                                ),
                                target_address.0,
                            );
                            None
                        })

                    }
                )*

                ::tracing::debug!(
                    "no inspector for function selector: {:?} with contract address: {:?}",
                    ::malachite::strings::ToLowerHexString::to_lower_hex_string(
                        &hex_selector
                    ),
                    target_address.0,
                );

                None
            }
        }
    )
    .into()
}

struct ActionDispatch {
    // required for all
    struct_name: Ident,
    rest:        Vec<Ident>,
}
impl Parse for ActionDispatch {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let struct_name: Ident = input.parse()?;
        let mut rest = Vec::new();
        while input.parse::<Token![,]>().is_ok() {
            rest.push(input.parse::<Ident>()?);
        }
        if !input.is_empty() {
            panic!("unkown characters")
        }

        Ok(Self { rest, struct_name })
    }
}
